from dotenv import load_dotenv
import os
from langchain_openai import ChatOpenAI
from query import SQLHandler, MongoHandler
import gradio as gr
import concurrent.futures
import traceback
import pandas as pd
from tabulate import tabulate
import json
import ast
from bson import ObjectId

# Load API key from .env
load_dotenv()
openai_api_key = os.getenv("KEY")

# Initialize LLM
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, openai_api_key=openai_api_key)
ip = "18.217.76.1"

# Initialize handlers once
sql_handler = SQLHandler(llm, ip)
mongo_handler = MongoHandler(llm, ip, "Books")

# Timeout duration in seconds
QUERY_TIMEOUT = 30

def beautify_mongo_query(raw_dict_str):
    try:
        # Convert string to dict
        dict_obj = ast.literal_eval(raw_dict_str)
        # Convert dict to formatted JSON
        pretty_json = json.dumps(dict_obj, indent=1)
        return pretty_json
    except Exception as e:
        return f"Error formatting query: {str(e)}"

def beautify_mongo_docs(doc_string):
    try:
        # Define ObjectId to mimic bson.ObjectId behavior
        safe_globals = {"ObjectId": lambda x: ObjectId(x)}

        # Step 1: Evaluate the string with ObjectId support
        docs = eval(doc_string, safe_globals)

        # Step 2: Convert ObjectId to string
        def clean(doc):
            return {
                k: str(v) if isinstance(v, ObjectId) else v
                for k, v in doc.items()
            }

        cleaned_docs = [clean(d) for d in docs]

        # Step 3: Pretty-print as JSON
        return json.dumps(cleaned_docs, indent=1)

    except Exception as e:
        return f"Error occurred: {e}"

def process_output(result, db_type, query_type):
    if db_type == "MySQL":
        if query_type == "select":
            print(result["sql_result"])
            df = pd.DataFrame(result["sql_result"], columns=result.get("columns", []))
            pretty_table = tabulate(df, headers='keys', tablefmt='github', showindex=False)
            print(pretty_table)
            return result["query"], pretty_table
        elif query_type == "modification":
            output_message = str(result["rows_mod"]) + " rows were affected."
            return result["query"], output_message
    elif db_type == "MongoDB":
        if query_type == "schema":
            return "The user requested schema information...", result["result"]
        elif query_type == "query":
            return beautify_mongo_query(str(result["mongo_query"])), beautify_mongo_docs(str(result["output"]))
        elif query_type == "modification":
            output_message = str(result["rows_mod"]) + " rows were affected."
            return str(result["mongo_query"]), output_message

def run_query(natural_language_input, database_type):
    try:
        def safe_query():
            if database_type == "Music DB (MySQL)":
                return sql_handler.query(natural_language_input), "MySQL"
            elif database_type == "Books DB (MongoDB)":
                return mongo_handler.query(natural_language_input), "MongoDB"
            else:
                return {"query": "", "sql_result": "Invalid database selection."}, database_type

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(safe_query)
            result, db_type = future.result(timeout=QUERY_TIMEOUT)
            result_query, result_output = process_output(result, db_type, result["intent"])
            return result_query, result_output

    except concurrent.futures.TimeoutError:
        return "query logic", "Query took too long and was canceled. Try a simpler or more specific question."
    except Exception as e:
        return "query logic", f"An error occurred: {str(e)}"

custom_css = """
footer {visibility: hidden}
#run-button {
    background-color: orange !important;
    color: white !important;
    border: none;
}
"""

with gr.Blocks(title="Natural Language Database Query", css=custom_css) as demo:
    gr.Markdown("<h1 style='text-align: center;'>Natural Language Database Query</h1>")
    gr.Markdown("<p style='text-align: center;'>Enter a question and choose which database to query from.</p>")

    with gr.Row(elem_id="input-row"):
        with gr.Column(scale=1):
            user_input = gr.Textbox(lines=2, placeholder="Ask a question...", label="Your Input")
            db_choice = gr.Dropdown(choices=["Music DB (MySQL)", "Books DB (MongoDB)"], label="Select Database")
            run_button = gr.Button("Run Query", elem_id="run-button")

    with gr.Row(elem_id="output-row"):
        generated_query = gr.Code(label="Generated Query")
        results = gr.Code(label="Results")

    run_button.click(
        fn=run_query,
        inputs=[user_input, db_choice],
        outputs=[generated_query, results]
    )

demo.launch()