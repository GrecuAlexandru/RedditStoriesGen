from openai import OpenAI
from pathlib import Path
import json
import os
import re
from time import sleep, time

import openai
import tiktoken
import yaml

from ShortGen.config.api_key_manager import ApiKeyManager


def num_tokens_from_messages(texts, model="gpt-4o-mini"):
    """Returns the number of tokens used by a list of messages."""
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        encoding = tiktoken.get_encoding("cl100k_base")
    if model == "gpt-4o-mini":  # note: future models may deviate from this
        if isinstance(texts, str):
            texts = [texts]
        score = 0
        for text in texts:
            score += 4 + len(encoding.encode(text))
        return score
    else:
        raise NotImplementedError(
            f"""num_tokens_from_messages() is not presently implemented for model {model}.
        See https://github.com/openai/openai-python/blob/main/chatml.md for information"""
        )


def extract_biggest_json(string):
    json_regex = r"\{(?:[^{}]|(?R))*\}"
    json_objects = re.findall(json_regex, string)
    if json_objects:
        return max(json_objects, key=len)
    return None


def get_first_number(string):
    pattern = r"\b(0|[1-9]|10)\b"
    match = re.search(pattern, string)
    if match:
        return int(match.group())
    else:
        return None


def load_yaml_file(file_path: str) -> dict:
    """Reads and returns the contents of a YAML file as dictionary"""
    return yaml.safe_load(open_file(file_path))


def load_json_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        json_data = json.load(f)
    return json_data


def load_local_yaml_prompt(file_path):
    _here = Path(__file__).parent
    _absolute_path = (_here / ".." / file_path).resolve()
    json_template = load_yaml_file(str(_absolute_path))
    return json_template["chat_prompt"], json_template["system_prompt"]


def open_file(filepath):
    with open(filepath, "r", encoding="utf-8") as infile:
        return infile.read()


def llm_completion(
    chat_prompt="",
    system="",
    temp=0.7,
    max_tokens=200,
    remove_nl=True,
    conversation=None,
):
    openai_key = ApiKeyManager.get_api_key("OPENAI_API_KEY")
    gemini_key = ApiKeyManager.get_api_key("GEMINI_API_KEY")
    if gemini_key:
        client = OpenAI(
            api_key=gemini_key,
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
        )
        model = "gemini-2.0-flash-lite-preview-02-05"
    elif openai_key:
        client = OpenAI(api_key=openai_key)
        model = "gpt-4o-mini"
    else:
        raise Exception("No OpenAI or Gemini API Key found for LLM request")
    max_retry = 5
    retry = 0
    error = ""
    for i in range(max_retry):
        try:
            if conversation:
                messages = conversation
            else:
                messages = [
                    {"role": "system", "content": system},
                    {"role": "user", "content": chat_prompt},
                ]
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temp,
                timeout=30,
            )
            text = response.choices[0].message.content.strip()
            if remove_nl:
                text = re.sub("\s+", " ", text)
            filename = "%s_llm_completion.txt" % time()
            if not os.path.exists(".logs/gpt_logs"):
                os.makedirs(".logs/gpt_logs")
            with open(".logs/gpt_logs/%s" % filename, "w", encoding="utf-8") as outfile:
                outfile.write(
                    f"System prompt: ===\n{system}\n===\n"
                    + f"Chat prompt: ===\n{chat_prompt}\n===\n"
                    + f"RESPONSE:\n====\n{text}\n===\n"
                )
            return text
        except Exception as oops:
            retry += 1
            print("Error communicating with OpenAI:", oops)
            error = str(oops)
            sleep(1)
    raise Exception(
        f"Error communicating with LLM Endpoint Completion errored more than error: {error}"
    )


def process_script_for_voice(script_text):
    """
    Process a script through GPT to improve readability, correct spelling/grammar,
    and add proper punctuation and line breaks for natural speech.
    """
    system_prompt = """
    You are an expert editor who specializes in preparing scripts for voice narration.
    Your task is to:
    1. Correct any spelling or grammar mistakes
    2. Add proper punctuation that makes the script flow naturally when read aloud
    3. Add appropriate line breaks where a speaker would naturally pause
    4. Maintain the original meaning and content of the script

    Do NOT add any new information or change the substance of the content.
    Your output should contain ONLY the edited text.
    """

    chat_prompt = f"Please edit this script to make it sound natural when read aloud:\n\n{script_text}"

    return llm_completion(
        chat_prompt=chat_prompt,
        system=system_prompt,
        max_tokens=1000,  # Increase token limit for longer scripts
        remove_nl=False,  # Keep line breaks
    )
