"""CLI entry point for alteryx2dbx."""
import click
from pathlib import Path
from .parser.xml_parser import parse_yxmd
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
