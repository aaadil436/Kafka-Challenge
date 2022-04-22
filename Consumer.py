import json

from kafka import KafkaConsumer


TOPIC = 'au.com.eliiza.newsheadlines.txt'
GROUP = 'my-group'
BOOTSTRAP_SERVER = 'localhost:9092'


# characters to alphabets
morse_alphabet = {
    "A": ".-",
    "B": "-...",
    "C": "-.-.",
    "D": "-..",
    "E": ".",
    "F": "..-.",
    "G": "--.",
    "H": "....",
    "I": "..",
    "J": ".---",
    "K": "-.-",
    "L": ".-..",
    "M": "--",
    "N": "-.",
    "O": "---",
    "P": ".--.",
    "Q": "--.-",
    "R": ".-.",
    "S": "...",
    "T": "-",
    "U": "..-",
    "V": "...-",
    "W": ".--",
    "X": "-..-",
    "Y": "-.--",
    "Z": "--..",
    " ": "/",
    "1": ".----",
    "2": "..---",
    "3": "...--",
    "4": "....-",
    "5": ".....",
    "6": "-....",
    "7": "--...",
    "8": "---..",
    "9": "----.",
    "0": "-----",
    ".": ".-.-.-",
    ",": "--..--",
    ":": "---...",
    "?": "..--..",
    "'": ".----.",
    "-": "-....-",
    "/": "-..-.",
    "@": ".--.-.",
    "=": "-...-"
}

# Morse codes to characters
inverse_morse_alphabet = dict((v, k) for (k, v) in morse_alphabet.items())


# a function that receives morse message and decodes it
def decrypt_morse_coded_message(morse_message):
    # initialize decoded message
    decoded_message = ''
    # words separated by a slash
    words = morse_message.split('/')
    for word in words:
        # dots and dashes with characters separated by a blank
        characters = word.split(' ')
        for char in characters:
            if char in inverse_morse_alphabet:
                decoded_message += inverse_morse_alphabet[char]
            else:
                # CNF = Character not found
                decoded_message += '<CNF>'
        decoded_message += ' '
    return decoded_message


def check_word_and_save_results(data):
    result = []
    for item in data:
        for word in item.split(' '):
            if word == 'AUSTRALIA':
                result.append(item)
    # Insert Data Into File
    with open('./output.txt', 'a') as f:
        f.write('\n'.join(result))
        f.close()
    print('output.txt File Created!')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Decoded data
    decoded_data = []
    # consume earliest available messages
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=[BOOTSTRAP_SERVER], auto_offset_reset='earliest',
                             value_deserializer=lambda v: v.decode("utf-8"), group_id=GROUP,
                             enable_auto_commit=True)
    for message in consumer:
        print('Decoding message at offset', message.offset)
        decoded_data.append(decrypt_morse_coded_message(message.value))
        # Decode only first 1000 lines
        if len(decoded_data) >= 1000:
            break

    check_word_and_save_results(decoded_data)
