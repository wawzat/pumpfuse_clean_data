# Coding Standards and Guidelines

## General Principles
- Code shall be written in Python and follow the PEP 8 style guide.
- Ensure that the code is compatible with Python 3.10 and above.
- Use descriptive variable and function names.
- Ensure that the code is accessible and understandable to other developers, providing context where necessary.
- Write PEP 257 compliant docstrings for all public modules, classes, functions, and methods.
- Ensure that the code is modular and reusable, with a focus on clarity and maintainability.
- Avoid using global variables unless absolutely necessary.
- Use type hints to clarify the expected types of function parameters and return values.

## Command Line Arguments
- Use the `argparse` library for any command line argument parsing.
- Ensure that command line arguments are well-documented and provide clear usage instructions.
- Validate command line arguments to ensure they meet expected formats and constraints. 
- Provide a -h and --help option to display usage information.

## Exception Handling
- All code must include exception handling.
- Use specific exception types whenever possible, and only use a general catch-all (e.g., `except Exception`) as a last resort.
- Always handle exceptions in a way that provides useful error messages and logging, and ensures the program can fail gracefully or recover where appropriate.
- For comaand line programs provide an exception for Ctrl-C to allow graceful shutdown.
- For programs to be run on a Raspberry Pi with GPIO, ensure that the program can handle GPIO cleanup on exit.

## Code Quality and Performance
- Optimize for performance where applicable, but prioritize readability and maintainability.
- Use comments to explain complex logic or decisions, but avoid obvious comments that do not add value.
- Use f-strings for string formatting where applicable for better readability.
- Maintain a consistent coding style throughout the project.

## Logging and Debugging
- Use logging instead of print statements for better debugging and traceability.

## Dependencies and Documentation
- Ensure that all dependencies are listed in `requirements.txt` for easy installation.
- Document the codebase with a `readme.md` file that includes setup instructions, usage examples, and any relevant information.

## Configuration and Security
- Avoid hardcoding values; use the `configparser` library with appripriate .ini files where appripriate.
- Use a configuration `config.ini` file for things like api keys and other sensitive information.
- Use a configuration file named `user_settings.ini` for non-sensitive user-specific settings.
- Add `config.ini` to `.gitignore` to prevent sensitive information from being committed to the repository.
- Ensure that the code is secure, avoiding common vulnerabilities such as SQL injection or insecure file handling.

## Portability
- Ensure that the code is portable and can run on different operating systems without modification.

## Commit Message Guidelines
- After completing an action that results in modifying a file, provide a text git commit message that describes the change made.
- The message should be concise yet descriptive enough to understand the modification without needing to look at the code.
- Follow a consistent commit message format, such as `feat:`, `fix:`, or `docs:`, to categorize changes.
- Do not provide a terminal command or any other text with the commit message, just the commit message itself.
- Use descriptive commit messages that explain the "why" behind changes, not just the "what".

## Preferred Libraries
- Use `TQDM` for progress bars in long-running operations.
- Use `requests` for HTTP requests.
- Use `pandas` for data manipulation and analysis.
- Use `gspread` for Google Sheets API interactions.
- Use `configparser` for reading configuration files.
- Use `argparse` for command line argument parsing.
- Use `logging` for logging and debugging.