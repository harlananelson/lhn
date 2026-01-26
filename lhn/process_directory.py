import os
import re

def replace_f_strings_in_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            content = file.read()

        original_content = content
        
        # Regex to find F' or F" potentially preceded by whitespace,
        # ensuring F is not part of an identifier.
        # It captures:
        # group 1: leading whitespace (if any)
        # group 2: F
        # group 3: the opening quote (' or ")
        
        # For F'
        # We want to replace F' with f'
        # The replacement string should be 'f' followed by the original quote.
        # So, if we match F and then ', the replacement is f and then '.
        
        # Pattern for F"
        # (?<!\w) ensures F is not preceded by a word character (safer than checking for specific non-characters)
        # (\s*) captures any leading whitespace before F
        # (F) captures F
        # (") captures the double quote
        content = re.sub(r"(\s*)(?<!\w)(F)(\")", r"\1f\3", content)
        
        # Pattern for F'
        # (\s*) captures any leading whitespace before F
        # (F) captures F
        # (') captures the single quote
        content = re.sub(r"(\s*)(?<!\w)(F)(\')", r"\1f\3", content)

        if content != original_content:
            with open(filepath, 'w', encoding='utf-8') as file:
                file.write(content)
            print(f"Modified: {filepath}")
        # else:
        #     print(f"No changes needed in: {filepath}")

    except Exception as e:
        print(f"Error processing file {filepath}: {e}")

def process_directory(directory_path):
    for root, _, files in os.walk(directory_path):
        for filename in files:
            if filename.endswith(".py"):
                filepath = os.path.join(root, filename)
                replace_f_strings_in_file(filepath)

# ---
# Before running this, ensure your ./lhn directory is clean or you have a backup.
# ---
# process_directory("./lhn")
# print("Processing complete. Please check your git diff.")

# Simple script to replace non-breaking spaces with regular spaces
import os
def normalize_whitespace_in_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            content = file.read()
        new_content = content.replace('\u00A0', ' ') # U+00A0 is non-breaking space
        if new_content != content:
            with open(filepath, 'w', encoding='utf-8') as file:
                file.write(new_content)
            print(f"Normalized whitespace in: {filepath}")
    except Exception as e:
        print(f"Error normalizing whitespace in {filepath}: {e}")

# for root, _, files in os.walk("./lhn"):
#     for filename in files:
#         if filename.endswith(".py"):
#             normalize_whitespace_in_file(os.path.join(root, filename))