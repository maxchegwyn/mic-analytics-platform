import sys

with open('/tmp/mic_token.json', 'rb') as f:
    content = f.read()

print('Length:', len(content))
print('Bytes 860-880:', content[860:880])
print('Last 10 bytes:', content[-10:])