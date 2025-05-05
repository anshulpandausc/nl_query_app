# Natural Language Database Query App

This project allows users to interact with MySQL and MongoDB databases using natural language, leveraging OpenAI's GPT models and a simple Gradio web interface.

---

## Features

- Query **MySQL** and **MongoDB** databases in natural language
- Powered by **OpenAI GPT-4o-mini**
- Built with **LangChain**, **Gradio**, and **SQLAlchemy**
- Automatically parses results into readable formats

---

## Project Installation

1. **Clone the repository**:

```bash
git clone git@github.com:anshulpandausc/nl_query_app.git
```

## Prerequisites

Before you begin, ensure you have the following:

- **Python 3.11.7** (or later within 3.x)
- **OpenAI API Key** (for natural language query interpretation)
    1. **Obtain an API Key**  
   - Sign in or sign up at [https://platform.openai.com/](https://platform.openai.com/)  
   - Go to your [API Keys page](https://platform.openai.com/account/api-keys)  
   - Click **"Create new secret key"** and copy the key

    2. **Create a `.env` File** in your project directory:  
   This file will securely store your API key as an environment variable.

   ```bash
   touch .env
   ```

   3. Add the API Key to .env
    
    ```
    OPENAI_API_KEY=your-api-key-here
    ```

   4. Ensure python-dotenv is Installed
      
    ```
    pip install python-dotenv
    ```

## Required Packages

After adding the API key, you will need to make sure the following packages are installed before running the project.

  - python-dotenv
  - openai
  - langchain
  - langchain-community
  - gradio
  - sqlalchemy
  - pymongo
  - tabulate
  - pandas
  - requests
  - bson
  - pymysql

The exact versions of each package can be found in the "requirements.txt" file. For automatic installation, you can run the following command:
```
pip install -r requirements.txt
```

---

## Running the Project

After the required packages have been installed, you will need to run the following to start the project:
```
python main.py
```

The terminal output will show a message like "Running on local URL:  http://127.0.0.1:7860". You will be able to access our web app style project by pasting this given link in the browser. It might take up to a minute for the url to show.

---

## Code Structure (Brief Overview)
The two files containing the code for our project are "main.py" and "query.py". "main.py" contains the frontend code and connects the backend with the user interface. The "query.py" is our backend code, and it contains a SQLHandler class and a MongoHandler class. In main.py, SQLHandler and MongoHandler objects are created to establish connections with our databases. Also, we initialized our llm in main.py, which is passed into the SQL and Mongo objects.

---
