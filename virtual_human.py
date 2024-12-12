import pygame
import pyttsx3
import threading
import time
from confluent_kafka import Consumer, KafkaException
from queue import Queue

# Pygame setup
pygame.init()
screen = pygame.display.set_mode((900, 600))
pygame.display.set_caption("Virtual Human")
clock = pygame.time.Clock()

# Load images
human_idle = pygame.image.load("human_idle.png")
human_speaking = pygame.image.load("human_speaking.png")

# Resize images for consistency
human_idle = pygame.transform.scale(human_idle, (400, 400))
human_speaking = pygame.transform.scale(human_speaking, (400, 400))

# Text-to-Speech setup
tts_engine = pyttsx3.init()
tts_lock = threading.Lock()  # Lock to manage access to TTS
message_queue = Queue()  # Queue for handling incoming messages

def tts_worker():
    """Thread worker for processing TTS messages from the queue."""
    while True:
        response = message_queue.get()  # Block until a message is available
        if response is None:  # Exit signal
            break
        with tts_lock:
            tts_engine.say(response)
            tts_engine.runAndWait()
        message_queue.task_done()

# Kafka setup
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'start_conversation'

consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'virtual_human_group',
    'auto.offset.reset': 'earliest'
}

# Kafka Consumer
try:
    consumer = Consumer(consumer_config)
    consumer.subscribe([KAFKA_TOPIC])
except KafkaException as e:
    print(f"Error initializing Kafka Consumer: {e}")
    consumer = None

# Variables for animation
is_speaking = False
response_text = ""
font = pygame.font.Font(None, 36)

def listen_to_kafka():
    """Listen for Kafka messages and enqueue responses."""
    global response_text

    if not consumer:
        print("Kafka Consumer not initialized. Exiting Kafka listener.")
        return

    while True:
        try:
            msg = consumer.poll(1.0)  # Poll for messages
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # Process the received message
            user_message = msg.value().decode('utf-8')
            print(f"Received from Kafka: {user_message}")

            # Generate response
            response_text = "Hello real human, how are you today!"

            # Enqueue the response for TTS
            message_queue.put(response_text)

        except Exception as e:
            print(f"Error during Kafka polling: {e}")

def simulate_speaking():
    """Simulate speaking animation."""
    global is_speaking
    while True:
        if not message_queue.empty():
            is_speaking = True
            time.sleep(0.1)  # Simulate speaking animation delay
            is_speaking = False

# Start Kafka listener in a separate thread
if consumer:
    kafka_thread = threading.Thread(target=listen_to_kafka, daemon=True)
    kafka_thread.start()

# Start TTS worker thread
tts_thread = threading.Thread(target=tts_worker, daemon=True)
tts_thread.start()

# Main loop
running = True
while running:
    screen.fill((255, 255, 255))  # White background

    # Display appropriate image
    if is_speaking:
        screen.blit(human_speaking, (250, 100))
    else:
        screen.blit(human_idle, (250, 100))

    # Display response text
    response_surface = font.render(response_text, True, (0, 0, 0))
    screen.blit(response_surface, (50, 500))

    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    pygame.display.flip()
    clock.tick(30)  # Limit to 30 FPS

pygame.quit()

# Shutdown threads gracefully
if consumer:
    consumer.close()
message_queue.put(None)  # Signal TTS worker to exit
tts_thread.join()
