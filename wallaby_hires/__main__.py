# __main__ is not required for DALiuGE components.
import argparse  # pragma: no cover

# from . import parset_mixin  # pragma: no cover


def main() -> None:  # pragma: no cover
    """
    The main function executes on commands:
    `python -m wallaby_hires` and `$ wallaby_hires `.

    This is your program's entry point.

    You can change this function to do whatever you want.
    Examples:
        * Run a test suite
        * Run a server
        * Do some other stuff
        * Run a command line application (Click, Typer, ArgParse)
        * List all available tasks
        * Run an application (Flask, FastAPI, Django, etc.)
    """
    parser = argparse.ArgumentParser(
        description="wallaby_hires.",
        epilog="Enjoy the wallaby_hires functionality!",
    )
    # This is required positional argument
    parser.add_argument(
        "name",
        type=str,
        help="The username",
        default="ICRAR",
    )
    # This is optional named argument
    parser.add_argument(
        "-m",
        "--message",
        type=str,
        help="The Message",
        default="Hello",
        required=False,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Optionally adds verbosity",
    )
    args = parser.parse_args()
    print(f"{args.message} {args.name}!")
    if args.verbose:
        print("Verbose mode is on.")

    print("Executing main function")
    # d0 = {"a": {"value": 1}, "b": {"value": 2}}
    # d1 = {"a": "Hello", "b": "World"}
    # print(parset_mixin(d0, d1))
    print("End of main function")


if __name__ == "__main__":  # pragma: no cover
    main()
