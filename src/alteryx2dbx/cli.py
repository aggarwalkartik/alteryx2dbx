"""CLI entry point for alteryx2dbx."""
import click
from pathlib import Path
from .parser.xml_parser import parse_yxmd
from .parser.unpacker import unpack_source
from .manifest import serialize_manifest
from .generator.notebook import generate_notebooks
from .generator.batch_report import generate_batch_report
from .dag.resolver import resolve_dag
from .handlers.registry import get_handler
from .parser.unpacker import unpack_source
import alteryx2dbx.handlers  # noqa: F401  — triggers registration


@click.group()
@click.version_option()
def main():
    """alteryx2dbx -- Convert Alteryx workflows to PySpark Databricks notebooks."""
    pass


@main.command()
@click.argument("source", type=click.Path(exists=True))
@click.option("-o", "--output", default="./manifest.json", help="Output path (file for single, dir for batch)")
def parse(source, output):
    """Parse .yxmd/.yxzp file(s) to JSON manifest(s)."""
    source_path = Path(source)
    output_path = Path(output)

    if source_path.is_file():
        # Single file mode
        unpacked = unpack_source(source_path)
        try:
            wf = parse_yxmd(unpacked.workflow_path)
            # Store macro/asset metadata in properties
            if unpacked.macros:
                wf.properties["macros"] = [str(m.name) for m in unpacked.macros]
            if unpacked.assets:
                wf.properties["assets"] = [str(a.name) for a in unpacked.assets]
            output_path.parent.mkdir(parents=True, exist_ok=True)
            serialize_manifest(wf, output_path)
            click.echo(f"Wrote {output_path}")
        finally:
            unpacked.cleanup()
    else:
        # Batch / directory mode
        files = list(source_path.glob("**/*.yxmd")) + list(source_path.glob("**/*.yxzp"))
        if not files:
            click.echo("No .yxmd/.yxzp files found.")
            return
        output_path.mkdir(parents=True, exist_ok=True)
        for f in files:
            unpacked = unpack_source(f)
            try:
                wf = parse_yxmd(unpacked.workflow_path)
                if unpacked.macros:
                    wf.properties["macros"] = [str(m.name) for m in unpacked.macros]
                if unpacked.assets:
                    wf.properties["assets"] = [str(a.name) for a in unpacked.assets]
                manifest_path = output_path / f"{f.stem}.json"
                serialize_manifest(wf, manifest_path)
                click.echo(f"Wrote {manifest_path}")
            except Exception as e:
                click.echo(f"Error parsing {f.name}: {e}", err=True)
            finally:
                unpacked.cleanup()
        click.echo(f"\nParsed {len(files)} workflow(s).")


@main.command()
@click.argument("source", type=click.Path(exists=True))
@click.option("-o", "--output", default="./output", help="Output directory")
@click.option("--report", is_flag=True, default=False, help="Generate aggregate batch_report.md")
def convert(source, output, report):
    """Convert .yxmd file(s) to Databricks notebooks."""
    source_path = Path(source)
    output_path = Path(output)
    if source_path.is_file():
        files = [source_path]
    else:
        files = list(source_path.glob("**/*.yxmd")) + list(source_path.glob("**/*.yxzp"))
    if not files:
        click.echo("No .yxmd files found.")
        return
    results = []
    for f in files:
        click.echo(f"Converting: {f.name}")
        unpacked = unpack_source(f)
        try:
            wf = parse_yxmd(unpacked.workflow_path)
            stats = generate_notebooks(wf, output_path)
            results.append(stats)
            click.echo(f"  Done: {output_path / wf.name}/")
        except Exception as e:
            click.echo(f"  Error: {e}", err=True)
            results.append({
                "name": f.stem,
                "tools_total": 0,
                "tools_converted": 0,
                "avg_confidence": 0,
                "unsupported_tools": [],
                "errors": [str(e)],
            })
        finally:
            unpacked.cleanup()
    if report:
        output_path.mkdir(parents=True, exist_ok=True)
        generate_batch_report(output_path, results)
        click.echo(f"Report: {output_path / 'batch_report.md'}")
    click.echo(f"\nDone. Converted {len(files)} workflow(s).")


@main.command()
@click.argument("source", type=click.Path(exists=True))
def analyze(source):
    """Analyze workflow without generating code."""
    source_path = Path(source)
    if source_path.is_file():
        files = [source_path]
    else:
        files = list(source_path.glob("**/*.yxmd")) + list(source_path.glob("**/*.yxzp"))
    for f in files:
        unpacked = unpack_source(f)
        try:
            wf = parse_yxmd(unpacked.workflow_path)
            order = resolve_dag(wf)
            click.echo(f"\nWorkflow: {wf.name}")
            click.echo(f"Tools: {len(wf.tools)}")
            supported = 0
            for tool_id in order:
                tool = wf.tools[tool_id]
                handler = get_handler(tool)
                is_supported = type(handler).__name__ != "UnsupportedHandler"
                if is_supported:
                    supported += 1
                status = "OK" if is_supported else "UNSUPPORTED"
                click.echo(f"  [{status}] [{tool_id}] {tool.tool_type}: {tool.annotation}")
            pct = supported / len(wf.tools) * 100 if wf.tools else 0
            click.echo(f"Coverage: {supported}/{len(wf.tools)} ({pct:.0f}%)")
        finally:
            unpacked.cleanup()


@main.command()
def tools():
    """List supported Alteryx tools."""
    from .handlers.registry import _registry
    click.echo("Supported tool types:")
    for tool_type in sorted(_registry._type_handlers.keys()):
        handler = _registry._type_handlers[tool_type]
        click.echo(f"  {tool_type} ({handler.__name__})")
    click.echo(f"\nTotal: {len(_registry._type_handlers)} tool types")
