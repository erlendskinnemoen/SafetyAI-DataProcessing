# Introduction 
This project leverages artificial intelligence to enhance the processing and analysis of improvement & safety reports, created onboard out vessels and saved to the improvement reporting system. By automating the process of improving the unstructured text description of an "event" and categorization of the data within these reports, it utilizes Azure's AI services to enhance text clarity and extract relevant topics used for gaining a deeper understanding of our organization's operational safety procedures. This system is designed to streamline interactions with the database, facilitating efficient data processing and generating actionable insights for safety management and decision-making.

## Key features
- AI-Powered Text Improvement: Utilizes Azure's OpenAI to refine the clarity, grammar, and technical precision of safety report narratives.
- Topic Extraction: Applies AI to categorize textual data, identifying key safety and improvement areas within reports.
- Database Integration: Streamlines the process of reading from and writing back to a PostgreSQL database, ensuring data integrity and structure compliance.
- Concurrent Processing: Leverages Python's concurrent.futures for efficient handling of multiple reports, enhancing processing speed.
- Error Handling: Robust mechanisms to manage AI processing failures, with retries and logging for transparency and debugging.

# Getting Started
1. Prerequisites: 
    - Python 3.8 or later.
    - Access to the internal table in the database.
    - Access (api_key/api_endpoint) to the Azure OpenAI service.
2.	Software dependencies:
    - All required dependencies are marked in the `requirements.txt` file, which includes:
        - Python Standard Library modules.
        - Pip-packages: run the command `pip install -r requirements.txt` to install necessary pip-packages (with a supported package version).
3.	Latest releases
    - Be especially aware of any changes published by either the company OpenAI or by Azure in regards to model parameters or structure.
4.	API references
    - [OpenAI Documentation](https://platform.openai.com/docs/introduction)
    - [OpenAI API chat documentation](https://platform.openai.com/docs/api-reference/chat)
    - [Azure OpenAI Service documentation](https://learn.microsoft.com/en-us/azure/ai-services/openai/)


# Build and Test
Verify that the variables in `dependencies.py` are accurate (e.g related to parallel processing, dotenv_path and .env(without placeholders), database destination table, etc.)

Run `main.py` in order to process the report 

# Detailed Model Descriptions
## main.py
The main.py script acts as the orchestrator for the entire process of analyzing maritime improvement reports. It coordinates the flow from reading data from the database, processing text through Azure OpenAI's GPT for enhancements and topic extraction, and finally writing the processed data back to the database. It uses the ThreadPoolExecutor for parallel processing to enhance performance. Key stages include data deduplication, text improvement, topic parsing, data formatting, and database updates. The script also defines the log config to monitor the progress and catch any issues during execution.
- Error handling: Implements logging at various stages to capture and review any issues encountered during the data processing pipeline. Failed operations on text improvements and topic parsing are tracked and saved to CSV files (improved_text_failed_rows.csv and parsed_topics_failed_rows.csv) for further analysis and rectification.

## dependencies.py
The dependencies.py file serves as a central hub for managing external service interactions, particularly with Azure OpenAI, and defines essential configurations, such as database queries and AI prompts. It loads environmental variables for secure API access, outlines SQL scripts for data selection and insertion, and holds the system prompts used for text improvement and topic extraction. 

## database_connection.py
database_connection.py is responsible for all direct interactions with the PostgreSQL database. It includes functions for establishing database connections, executing read and write operations, and ensuring data is correctly formatted to meet database schema requirements. It leverages `psycopg2` and `SQLAlchemy` for robust database operations. When writing to the database the script uses an UPSERT functionality to update already existing rows and adding the new ones. 
- Error handling: Handles database connection errors, query execution failures, and ensures that any data writing issues are logged. It uses Python's exception handling mechanisms to manage unexpected database errors, providing detailed logs for troubleshooting.

## improve_text_gpt_35_turbo.py
improve_text_gpt_35_turbo.py utilizes Azure OpenAI's GPT (generative pretrained transformer) model to improve and clarify given input text, currently improvement reports from K-fleet with a focus on safety content. It takes an input text and provides a version of the text that has been improved in terms of clarity, technical language, grammar, and spelling. The new text is presented as a new column to the input file.
- Error handling: The `aiImprovedResponse` function tries to handle errors related to http, service requests, timeout and bad request errors. If any row uses more than max_tries it’s saved to a csv file (improved_text_failed_rows.csv), available for futher inspection. 

## parse_topics_gpt_35_turbo.py
parse_topics_gpt_35_turbo.py utilizes Azure OpenAI's GPT to extract topics from blocks of text. It is designed for parallel processing and manages potential API errors with retries. The core objective is choosing a set of key topics (from the `categories.txt`file) related to safety for personnel and material by analyzing the unstructured text data. After processing each chunk, the script introduces a brief pause (sleep) before moving on to the next chunk. The purpose of the sleep is to not overloading the openAI server, which wil trigger a limit error. 
- Error handling: The `aiTopicResponse` function tries to handle errors related to http, service requests, timeout and bad request errors. If any row uses more than max_tries it’s saved to a csv file (parsed_topics_failed_rows.csv), available for futher inspection. 

# Contribute
## TODO's
TODO: Deploy to Azure Kubernetes Service

If you want to learn more about creating good readme files then refer the following [guidelines](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops). You can also seek inspiration from the below readme files:
- [ASP.NET Core](https://github.com/aspnet/Home)
- [Visual Studio Code](https://github.com/Microsoft/vscode)
- [Chakra Core](https://github.com/Microsoft/ChakraCore)