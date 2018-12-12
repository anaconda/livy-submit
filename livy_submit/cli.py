from argparse import ArgumentParser
from os.path import expanduser


def make_parser():
    base = _base_parser()
    _livy_info(base)
    return base


def _base_parser():
    """Configure the base parser that the other subcommands will inherit from

    Returns
    -------
    ArgumentParser
    """
    ap = ArgumentParser(
        prog='livy-submit',
        description="CLI for interacting with the Livy REST API",
        add_help=False
    )
    ap.add_argument(
        '--conf-file',
        action='store',
        default=expanduser('~/.livy-submit'),
        help="The location of the livy submit configuration file"
    )
    return ap


def _livy_info(base_parser):
    """Configure the `livy info` subparser

    Parameters
    ----------
    base_parser

    Returns
    -------
    ArgumentParser
    """

    ap = ArgumentParser(parents=[base_parser])
    ap.add_argument(
        '--short',
        action='store_true',
        default=False,
        help="Only show the current status of the job"
    )
    ap.add_argument(
        'batchId',
        action='store',
        # required=False,
        help="The Livy batch ID that you want information for"
    )


if __name__ == "__main__":
    ap = make_parser()
    args = ap.parse_args()
    print(args)
