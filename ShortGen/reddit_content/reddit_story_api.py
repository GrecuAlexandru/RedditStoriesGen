import re
import json
import os


def load_abbreviations():
    """Load abbreviations from JSON file."""
    try:
        # abbreviations.json is in the root directory of the project
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up two levels to reach project root
        root_dir = os.path.dirname(os.path.dirname(current_dir))
        json_path = os.path.join(root_dir, "abbreviations.json")
        print(json_path)

        with open(json_path, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        print("Warning: abbreviations.json file not found.")
        return {}
    except json.JSONDecodeError:
        print("Warning: abbreviations.json is not valid JSON.")
        return {}


def expand_abbreviations(text):
    """Replace abbreviations with their expanded forms."""
    # Load abbreviations from JSON
    abbreviations = load_abbreviations()
    if not abbreviations:
        return text

    # Process the text word by word
    words = text.split()
    for i, word in enumerate(words):
        # Strip punctuation for checking
        clean_word = word.strip(".,!?;:()\"'")
        upper_word = clean_word.upper()

        # If the cleaned word is an abbreviation, replace it
        if upper_word in abbreviations:
            # Preserve original punctuation
            prefix = ""
            suffix = ""

            # Extract any leading punctuation
            while word and not word[0].isalnum():
                prefix += word[0]
                word = word[1:]

            # Extract any trailing punctuation
            while word and not word[-1].isalnum():
                suffix = word[-1] + suffix
                word = word[:-1]

            # Replace with expanded form, preserving case if possible
            if word.isupper():
                replacement = abbreviations[upper_word]
            elif word[0].isupper() and word[1:].islower():
                replacement = abbreviations[upper_word].capitalize()
            else:
                replacement = abbreviations[upper_word].lower()

            words[i] = prefix + replacement + suffix

    return " ".join(words)


def get_reddit_post_content(url):
    import praw

    # Replace these with your Reddit app credentials
    client_id = "TmOhZT2yqiVB7ohbrICt7w"
    client_secret = "Gg029AC-plh9_U2qXqL5SEtOKriXnQ"
    user_agent = "ShortGen/0.1 by Expert_Ad8726"

    # Initialize the Reddit instance
    reddit = praw.Reddit(
        client_id=client_id, client_secret=client_secret, user_agent=user_agent
    )

    # Extract the post ID from the URL
    match = re.search(r"comments/([a-z0-9]+)", url)
    if not match:
        return None

    post_id = match.group(1)

    # Fetch the submission using the post ID
    submission = reddit.submission(id=post_id)

    # Retrieve the title, author, and content
    title = submission.title
    author = submission.author.name if submission.author else "[deleted]"
    content = submission.selftext

    # Expand abbreviations in title and content
    expanded_title = expand_abbreviations(title)
    expanded_content = expand_abbreviations(content)

    # Ensure first letter of title and content is uppercase
    def capitalize_first_letter(text):
        if not text:
            return text
        # Find the first alphabetic character
        match = re.search(r"[a-zA-Z]", text)
        if match:
            index = match.start()
            # Replace the first letter with uppercase
            return text[:index] + text[index].upper() + text[index + 1 :]
        return text

    expanded_title = capitalize_first_letter(expanded_title)
    expanded_content = capitalize_first_letter(expanded_content)

    return expanded_title, author, expanded_content
