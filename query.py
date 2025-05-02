from langchain_community.utilities import SQLDatabase
from langchain.callbacks.base import BaseCallbackHandler
from sqlalchemy import create_engine
import re
from pymongo import MongoClient
import json
from sqlalchemy.sql import text
import requests
import urllib.parse

## SQL HANDLER
class SQLHandler(BaseCallbackHandler):
    def __init__(self, llm, ip):
        self.ip = ip
        DB_USER = "test"
        DB_PASSWORD = "test"
        DB_HOST = ip
        DB_PORT = "3306"
        DB_NAME = "Music"

        self.db_uri = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        self.engine = create_engine(self.db_uri)
        self.db = SQLDatabase.from_uri(self.db_uri)
        self.llm = llm

        self.sql_result = []

    def on_agent_action(self, action, **kwargs):
        if action.tool in ["sql_db_query_checker", "sql_db_query"]:
            self.sql_result.append(action.tool_input)
    
    def query(self, query):
        context = f"""
        You are an AI assistant that determines the type of SQL query based on natural language instructions.

        Classify the following instruction as one of:
        - "modification" (if it involves INSERT, UPDATE, DELETE, CREATE, DROP, ALTER)
        - "select" (if it involves reading data with SELECT)

        Only return "modification" or "select" as the output. Do not explain or provide SQL code.

        Instruction: "{query}"
        """

        response = self.llm.invoke(context)
        response_text = response.content if hasattr(response, "content") else str(response)

        response_text = response_text.strip().lower()
        print("Detected SQL Intent:", response_text)

        if response_text == "select":
            output = self.run_select_query_direct(query)
            output["intent"] = "select"
            return output
        elif response_text == "modification":
            output = self.run_modification_query(query)
            output["intent"] = "modification"
            return output
        
        return "Unexpected output."

    def generate_select_query(self, natural_language_query):
        context = f"""
        You are an AI assistant that converts natural language instructions into SQL SELECT queries.

        MySQL Database: Music
        Tables and their columns:
        {self.db.get_table_info()}

        Important notes:
        - Tables may contain related data split across them (e.g., track metadata in one table and track audio features in another).
        - Be smart about using JOINs if required — for example, if a column is not found in one table, check if it's in another table and use a JOIN via a shared key (like album_id).
        - Always include table names when referencing columns (e.g., tracks.name, sounds.energy).
        - Return a valid SQL SELECT query that correctly retrieves the answer based on the available schema.
        - Only return the SQL query, no explanation or formatting.

        Convert the following instruction into a valid SQL SELECT query for MySQL.

        Only return the SQL query directly, without explanation or formatting.

        Instruction: "{natural_language_query}"
        """

        response = self.llm.invoke(context)
        response_text = response.content if hasattr(response, "content") else str(response)

        # Clean potential markdown formatting (```sql ... ```)
        response_text = re.sub(r"^```(?:sql)?|```$", "", response_text.strip(), flags=re.MULTILINE).strip()

        print("Generated SQL (SELECT):", response_text)
        return response_text
    
    def run_select_query_direct(self, natural_language_query):
        # Step 1: Generate SQL
        sql_query = self.generate_select_query(natural_language_query)

        sql_result = None
        # Step 2: Execute SQL
        if sql_query:
            with self.engine.begin() as connection:
                result = connection.execute(text(sql_query))
                if result.returns_rows:
                    sql_result = result.fetchall()
                    columns = result.keys()
                else:
                    sql_result = f"{result.rowcount} rows affected."

        return {
            "query": sql_query,
            "sql_result": sql_result,
            "columns": columns
        }

    def generate_modification_query(self, natural_language_query):
        print(self.db.get_table_info())
        context = f"""
        You are an AI assistant that converts natural language instructions into SQL data modification statements.

        MySQL Database: Music
        Tables and their columns:
        {self.db.get_table_info()}

        Important rules:
        - Use valid data types for all fields. If unsure, use reasonable defaults or mock values.
        - Only generate valid SQL statements for MySQL.
        - Only return the SQL statement. Do not explain or format with markdown.

        Convert the following instruction into a valid SQL statement for MySQL.

        Only return the SQL statement directly, without explanation or formatting.

        Instruction: "{natural_language_query}"
        """

        response = self.llm.invoke(context)
        response_text = response.content if hasattr(response, "content") else str(response)

        # Clean potential markdown formatting (```sql ... ```)
        response_text = re.sub(r"^```(?:sql)?|```$", "", response_text.strip(), flags=re.MULTILINE).strip()

        print("LLM Response (SQL):", response_text)
        return response_text
    
    def run_modification_query(self, natural_language_query):
        try:
            sql = self.generate_modification_query(natural_language_query)

            if not sql.lower().startswith(("insert", "update", "delete")):
                raise ValueError("Detected a non-modification query. Only INSERT, UPDATE, DELETE are allowed.")

            with self.engine.begin() as conn:
                result = conn.execute(text(sql))
                return {"query": sql, "rows_mod": result.rowcount}
                
        except Exception as e:
            return "There was an error. "+str(e)


## MONGO HANDLER
class MongoHandler:
    def __init__(self, llm, ip, db_name):
        self.ip = ip
        self.db_name = db_name

        MONGO_URI = "mongodb://"+str(self.ip)
        DB_NAME = db_name

        self.client = MongoClient(MONGO_URI)
        self.db = self.client[DB_NAME]
        self.llm = llm

        self.collection_attributes = self._get_schema()

    def _get_schema(self):
        schema = {}
        collection_names = self.db.list_collection_names()
        for collection in collection_names:
            sample_doc = self.db[collection].find_one()
            if sample_doc:
                schema[collection] = list(sample_doc.keys())
        return schema


    def generate_query(self, natural_language_query):
        """
        Enhanced universal query generator that handles all query types:
        - Basic find operations with projections
        - Join operations using $lookup
        - Group/aggregation operations
        - Advanced operations like $sort, $skip, $limit
        """
        context = f"""
        You are an AI assistant that converts natural language queries into valid MongoDB aggregation queries.

        MongoDB Database: {self.db.name}
        Collections and their attributes:
        {json.dumps(self.collection_attributes, indent=2)}

        Convert the following question into a valid MongoDB aggregation query.
        The query **must automatically determine the correct collection**.
        
        Consider the query type based on these patterns:
        - If it involves joining or combining data from multiple collections, use $lookup
        - If it involves calculating averages, counts, sums, or grouping data, use $group
        - If it involves filtering specific documents, use $match
        - If it involves sorting, use $sort (1 for ascending, -1 for descending)
        - If it involves limiting results, use $limit
        - If it involves skipping results, use $skip
        - If it involves selecting specific fields, use $project

        Always construct MongoDB aggregation pipelines with the correct order of stages. For example, when retrieving the Nth highest value, use $sort followed by $skip and then $limit to ensure accurate results.
        
        The output format must be a valid JSON object with this structure:
        
        {{
            "collection": "name_of_selected_collection",
            "aggregate": [ {{ aggregation_pipeline }} ]
        }}
        
        Do NOT return extra text or explanations—only the JSON object.
        
        Question: "{natural_language_query}"
        """

        response = self.llm.invoke(context)
        response_text = response.content if hasattr(response, "content") else str(response)
        response_text = re.sub(r"^```(?:json)?|```$", "", response_text.strip(), flags=re.MULTILINE).strip()

        try:
            query_object = json.loads(response_text)
            if "collection" in query_object and "aggregate" in query_object:
                return query_object
            else:
                print("Error: LLM response does not contain expected structure.")
                return None
        except json.JSONDecodeError:
            print("Error: LLM did not return a valid JSON response.")
            print("Invalid JSON:", response_text)
            return None

    def generate_modification_query(self, natural_language_query):
        context = f"""
        You are an AI assistant that converts natural language instructions into MongoDB data modification operations.

        MongoDB Database: {self.db.name}
        Collections and their attributes:
        {json.dumps(self.collection_attributes, indent=2)}

        Convert the following instruction into a valid MongoDB data modification JSON.

        Keep in mind: 
        - If values like bookID or ISBN are required, generate valid-looking dummy values (e.g., '1', 'ISBN001'), not placeholders like 'unique_id' or 'isbn_of_the_book'.

        Output format:
        {{
            "operation": "insertOne" | "insertMany" | "updateOne" | "deleteOne" | "updateMany" | "deleteMany",
            "collection": "collection_name",
            "filter": {{ optional for delete/update }},
            "update": {{ optional for update }},
            "data": {{ optional for insert }}
        }}

        Instruction: "{natural_language_query}"
        """
        response = self.llm.invoke(context)
        response_text = response.content if hasattr(response, "content") else str(response)

        response_text = re.sub(r"^```(?:json)?|```$", "", response_text.strip(), flags=re.MULTILINE).strip()

        print("Cleaned LLM Response:", response_text)

        try:
            mod_query = json.loads(response_text)
            if "operation" in mod_query and "collection" in mod_query:
                return mod_query
            else:
                print("Error: LLM response does not contain expected structure.")
                return None
        except json.JSONDecodeError as e:
            print("Error: LLM did not return a valid JSON response.")
            print("Parsing error:", e)
            return None
    
    def describe_schema(self, natural_language_query):
        # Construct the context to send to the LLM
        context = {
            "database": self.db.name,
            "collections": self.collection_attributes
        }
        
        # Prepare the LLM prompt
        prompt = f"""
        You are an AI assistant helping with MongoDB database queries. Given the following information about a database, answer the user's question:

        Database: {context['database']}
        Collections and their attributes:
        {context['collections']}

        User's Query: "{natural_language_query}"

        Please respond with a detailed and informative answer based on the above context. If the question asks about collections, attributes, or any other schema-related details, provide the appropriate response.
        Don't provide unnecessary information and keep the response brief and concise. If the query is not recognized as a schema-related question, respond with "Unable to process query."
        """

        # Call the LLM to get the response
        response = self.llm.invoke(prompt)
        out = response.content if hasattr(response, 'content') else 'No content available'

        return out
    
    def modify_data(self, nl_instruction):
        mod_query = self.generate_modification_query(nl_instruction)
        if not mod_query:
            return None

        c, op = self.db[mod_query["collection"]], mod_query["operation"]

        try:
            if op == "insertOne":
                c.insert_one(mod_query["data"])
                count = 1
            elif op == "insertMany":
                count = len(c.insert_many(mod_query["data"]).inserted_ids)
            elif op == "updateOne":
                count = c.update_one(mod_query["filter"], mod_query["update"]).modified_count
            elif op == "updateMany":
                count = c.update_many(mod_query["filter"], mod_query["update"]).modified_count
            elif op == "deleteOne":
                count = c.delete_one(mod_query["filter"]).deleted_count
            elif op == "deleteMany":
                count = c.delete_many(mod_query["filter"]).deleted_count
            else:
                return None

            return {"mongo_query": mod_query, "rows_mod": count, "intent":"modification"}

        except Exception:
            return None

    
    def query(self, nl_query):
        """
        Determine the intent of a MongoDB natural language query and route to appropriate handler.
        """
        context = f"""
        You are an AI assistant that determines the type of MongoDB operation based on natural language instructions.
        Classify the following instruction as one of:
        - "schema" – if it asks about the database structure, such as showing which collections exist, what attributes (fields) are in each collection, or general questions about the design or metadata of the database
        - "modification" (if it involves adding, inserting, updating, or deleting data)
        - "query" (if it involves finding, retrieving, joining, or aggregating data)
        Only return "schema", "modification", or "query" as the output. Do not explain.
        Instruction: "{nl_query}"
        """
        response = self.llm.invoke(context)
        response_text = response.content if hasattr(response, "content") else str(response)
        response_text = response_text.strip().lower()
        print("Detected MongoDB Intent:", response_text)
        
        if response_text == "schema":
            output = {"intent": "schema"}
            output["result"] = self.describe_schema(nl_query)
            return output
        elif response_text == "modification":
            output = self.modify_data(nl_query)
            return output
        elif response_text == "query":
            # This is the consolidated query function we already built
            mongo_query = self.generate_query(nl_query)
            if not mongo_query:
                print("Invalid query.")
                return None

            try:
                collection_name = mongo_query.get("collection")
                pipeline = mongo_query.get("aggregate", [])

                if not collection_name or collection_name not in self.db.list_collection_names():
                    print(f"Error: Collection '{collection_name}' does not exist.")
                    return None
                if not isinstance(pipeline, list) or not pipeline:
                    print("Error: Invalid or empty aggregation pipeline.")
                    return None

                results = list(self.db[collection_name].aggregate(pipeline))
                return {"mongo_query": mongo_query, "pipeline_steps": pipeline, "output": results, "intent": "query"}

            except Exception as e:
                print(f"Error executing MongoDB query: {e}")
                return None
        else:
            return "Unexpected intent classification."