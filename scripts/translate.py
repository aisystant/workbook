#!/bin/env python3

import requests
import logging
import os
import sys
import yaml
import json
import base64
from langsmith import Client
from openai import OpenAI
from langchain_core.messages import convert_to_openai_messages

import dotenv

# Load environment variables from .env file
dotenv.load_dotenv()


# Configure logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=log_level,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


client = Client()           # LangSmith client
oai_client = OpenAI()       # OpenAI client

prompt = client.pull_prompt("exercisetest")


def translate_text(text, lang):
    """
    Translate the given text using the OpenAI API.
    """
    logger.info(f"Translating text: {text[:100]}... to {lang}")
    if lang != "en":
        raise NotImplementedError("Only English translation is supported.")
    doc = {
        "body": text,
    }
    formatted_prompt = prompt.invoke(doc)
    response = oai_client.chat.completions.create(
        model="gpt-4o",  # Change to "gpt-4" if needed
        messages=convert_to_openai_messages(formatted_prompt.messages)
    )
    translated_text = response.choices[0].message.content.strip()
    logger.info(f"Translated text: {translated_text[:100]}...")
    if not translated_text:
        logger.error("Translation returned an empty result.")
        sys.exit(1)
    return translated_text


def translate_yaml_file(src_filename, dst_filename, lang):
    """
    Translate the given YAML file using the OpenAI API.
    """
    logger.info(f"Translating file: {src_filename} to {dst_filename}")
    with open(src_filename, "r") as f:
        content = f.read()
    translated_content = translate_text(content, lang)
    with open(dst_filename, "w") as out_f:
        out_f.write(translated_content)


# iterate over all files in the directory
workbook_name = sys.argv[1]
logger.info(f"Translating workbook: {workbook_name}")
src_dir = os.path.join("ru", workbook_name, "tables")
dst_dir = os.path.join("en", workbook_name, "tables")
if not os.path.exists(dst_dir):
    os.makedirs(dst_dir)

for filename in os.listdir(src_dir):
    if filename.endswith(".yaml"):
        src_filename = os.path.join(src_dir, filename)
        dst_filename = os.path.join(dst_dir, filename)
        translate_yaml_file(src_filename, dst_filename, "en")
    else:
        logger.warning(f"Skipping non-YAML file: {filename}")
