# Agricultural Data Processing Application

## Overview

This application processes unstructured agricultural data in JSON format and transforms it into a structured format with proper data validation. It parses text descriptions of farming operations, extracts key information about operations, crops, areas, and yields, and produces a standardized JSON output.

## Features

- Extracts structured data from unstructured Russian text in JSON input
- Identifies operations, crops, divisions, areas, and yields
- Validates data against reference lists of valid values
- Handles errors and corrects common mistakes
- Formats output according to a standardized schema

## Requirements

- Python 3.7 or higher
- No external dependencies required (only standard library)

## Project Structure

```
agricultural_data_processor/
│
├── main.py                  # Main entry point
├── config/
│   └── reference_data.py    # Reference lists/dictionaries
│
├── utils/
│   ├── __init__.py
│   ├── input_processor.py   # Input JSON handling
│   ├── text_parser.py       # Text analysis functionality
│   ├── validator.py         # Data validation
│   ├── error_handler.py     # Error management
│   └── output_formatter.py  # Output JSON formatting
│
└── tests/
    ├── __init__.py
    ├── test_input.json      # Sample input
    ├── test_output.json     # Expected output
    └── test_parser.py       # Parser tests
```

## Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/agricultural-data-processor.git
cd agricultural-data-processor
```

2. No additional installation steps needed since the application uses only Python standard library.

## Usage

Run the application from the command line:

```bash
python main.py input.json --output output.json
```

Where:
- `input.json` is your input file
- `output.json` is the desired output file location (defaults to 'output.json' if not specified)

### Input Format

The application expects JSON input with this structure:
```json
{
  "messages": [
    {
      "id": 1,
      "date": "2025-04-12",
      "payload": "Пахота зяби под мн тр\nПо Пу 26/488\nОтд 12 26/221\n..."
    },
    ...
  ]
}
```

### Output Format

The application produces output in this structure:
```json
{
  "reports": [
    {
      "message_number": 1,
      "payload": "Пахота зяби под мн тр\nПо Пу 26/488\nОтд 12 26/221\n...",
      "parsed": [
        {
          "date": "12.04",
          "division": "АОР",
          "operation": "Пахота",
          "crop": "Многолетние травы текущего года",
          "dailyArea": 26,
          "totalArea": 488,
          "dailyYield": null,
          "totalYield": null
        },
        ...
      ]
    },
    ...
  ]
}
```

## How It Works

The application processes the data through several stages:

1. **Input Processing**: Loads and validates the input JSON
2. **Text Parsing**: Extracts structured information from the text
3. **Data Validation**: Validates extracted data against reference lists
4. **Error Handling**: Detects and corrects errors, deals with missing data
5. **Output Formatting**: Creates the final structured output

## Reference Data

The application validates extracted data against several reference lists:

1. **Valid Operations**: List of valid agricultural operations
2. **Division Structure**: Organization of divisions, regions, and departments
3. **Valid Crops**: List of valid agricultural crops

## Extending the Application

To add support for new data types:

1. **Add New Crop Types**: Update the reference lists in `reference_data.py`
2. **Support New Operations**: Add to the operations list in `reference_data.py`
3. **Handle New Text Patterns**: Extend the parsing logic in `text_parser.py`
4. **Add New Validation Rules**: Enhance validation in `validator.py`

## License

This project is licensed under the MIT License - see the LICENSE file for details.

