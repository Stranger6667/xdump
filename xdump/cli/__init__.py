import click
import yaml


@click.command()
@click.option('-c', '--config', help='Path to configuration file', type=click.File('r'))
@click.option('-v', '--verbosity', count=True, help='Verbosity level')
@click.version_option()
@click.pass_context
def xdump(ctx, config, verbosity):
    loaded_config = yaml.safe_load(config)
    backend = loaded_config['dump']['backend']
    module_name, class_name = backend.rsplit('.', 1)
    module = __import__(module_name, fromlist=[class_name])
    backend_class = getattr(module, class_name)
    backend_class(
        dbname=loaded_config['dump']['dbname'],
        user=loaded_config['dump']['user'],
        password=loaded_config['dump']['password'],
        host=loaded_config['dump']['host'],
        port=loaded_config['dump']['port'],
        verbosity=verbosity,
    ).dump(
        filename=loaded_config['dump']['output_file'],
        full_tables=loaded_config['dump']['full_tables'],
        partial_tables=loaded_config['dump']['partial_tables'],
    )
