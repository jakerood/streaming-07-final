"""
    This program listens for stock price messages on multiple queues continuously and checks for alerts.

    Author: Jake Rood
    Date: June 13, 2024

"""

import pika
import sys
import re
from collections import deque

# Import and configure the logger 
from util_logger import setup_logger
logger, logname = setup_logger(__file__)

# Define variables
aapl_queue = '01-aapl'
amzn_queue = '02-amzn'
goog_queue = '03-goog'
msft_queue = '04-msft'
stock_alert_threshold = 0.5 # Percentage change threshold for alerts
stock_window = 5 # Readings every minute seconds for 5 minutes = 5 readings

# Initialize the deques for stock prices
stock_prices = {
    aapl_queue: deque(maxlen=stock_window),
    amzn_queue: deque(maxlen=stock_window),
    goog_queue: deque(maxlen=stock_window),
    msft_queue: deque(maxlen=stock_window),
}

# Define callback functions to be called when a stock price message is received for each queue
# There are four different callback functions

# AAPL
def aapl_callback(ch, method, properties, body):
    """Callback function for AAPL queue"""
    logger.info(f" [x] Received from {aapl_queue}: {body.decode()}")
    try:
        match = re.match(r"AAPL price at (.*) is (.*)", body.decode())
        if not match:
            raise ValueError("Message format incorrect")
        timestamp = match.group(1)
        price = float(match.group(2))
    except (ValueError, IndexError) as e:
        logger.error(f"Failed to parse message: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    # Append the price to the queue's respective deque
    stock_prices[aapl_queue].append(price)

    # Check for price change alert
    if len(stock_prices[aapl_queue]) == stock_window:
        initial_price = stock_prices[aapl_queue][0]
        current_price = stock_prices[aapl_queue][-1]
        price_change = round(((current_price - initial_price) / initial_price) * 100, 2) # Percentage of stock price change
        if abs(price_change) >= stock_alert_threshold:
            logger.warning(f"PRICE ALERT at {timestamp} for AAPL! Price changed by {price_change}% in the last {stock_window} minutes.")
    
    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# AMZN
def amzn_callback(ch, method, properties, body):
    """Callback function for AMZN queue"""
    logger.info(f" [x] Received from {amzn_queue}: {body.decode()}")
    try:
        match = re.match(r"AMZN price at (.*) is (.*)", body.decode())
        if not match:
            raise ValueError("Message format incorrect")
        timestamp = match.group(1)
        price = float(match.group(2))
    except (ValueError, IndexError) as e:
        logger.error(f"Failed to parse message: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    # Append the price to the queue's respective deque
    stock_prices[amzn_queue].append(price)

    # Check for price change alert
    if len(stock_prices[amzn_queue]) == stock_window:
        initial_price = stock_prices[amzn_queue][0]
        current_price = stock_prices[amzn_queue][-1]
        price_change = round(((current_price - initial_price) / initial_price) * 100, 2) # Percentage of stock price change
        if abs(price_change) >= stock_alert_threshold:
            logger.warning(f"PRICE ALERT at {timestamp} for AMZN! Price changed by {price_change}% in the last {stock_window} minutes.")
    
    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# GOOG
def goog_callback(ch, method, properties, body):
    """Callback function for GOOG queue"""
    logger.info(f" [x] Received from {goog_queue}: {body.decode()}")
    try:
        match = re.match(r"GOOG price at (.*) is (.*)", body.decode())
        if not match:
            raise ValueError("Message format incorrect")
        timestamp = match.group(1)
        price = float(match.group(2))
    except (ValueError, IndexError) as e:
        logger.error(f"Failed to parse message: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    # Append the price to the queue's respective deque
    stock_prices[goog_queue].append(price)

    # Check for price change alert
    if len(stock_prices[goog_queue]) == stock_window:
        initial_price = stock_prices[goog_queue][0]
        current_price = stock_prices[goog_queue][-1]
        price_change = round(((current_price - initial_price) / initial_price) * 100, 2) # Percentage of stock price change
        if abs(price_change) >= stock_alert_threshold:
            logger.warning(f"PRICE ALERT at {timestamp} for GOOG! Price changed by {price_change}% in the last {stock_window} minutes.")
    
    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# MSFT
def msft_callback(ch, method, properties, body):
    """Callback function for MSFT queue"""
    logger.info(f" [x] Received from {msft_queue}: {body.decode()}")
    try:
        match = re.match(r"MSFT price at (.*) is (.*)", body.decode())
        if not match:
            raise ValueError("Message format incorrect")
        timestamp = match.group(1)
        price = float(match.group(2))
    except (ValueError, IndexError) as e:
        logger.error(f"Failed to parse message: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    # Append the price to the queue's respective deque
    stock_prices[msft_queue].append(price)

    # Check for price change alert
    if len(stock_prices[msft_queue]) == stock_window:
        initial_price = stock_prices[msft_queue][0]
        current_price = stock_prices[msft_queue][-1]
        price_change = round(((current_price - initial_price) / initial_price) * 100, 2) # Percentage of stock price change
        if abs(price_change) >= stock_alert_threshold:
            logger.warning(f"PRICE ALERT at {timestamp} for MSFT! Price changed by {price_change}% in the last {stock_window} minutes.")
    
    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# define a main function to run the program
def main(hn: str = "localhost"):
    """ Continuously listen for stock price messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        logger.error("ERROR: connection to RabbitMQ server failed.")
        logger.error(f"Verify the server is running on host={hn}.")
        logger.error(f"The error says: {e}")
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # Declare queues and set up consumption
        queues_callbacks = {
            aapl_queue: aapl_callback,
            amzn_queue: amzn_callback,
            goog_queue: goog_callback,
            msft_queue: msft_callback
        }

        for queue_name, callback in queues_callbacks.items():
            # Declare each queue as durable
            channel.queue_declare(queue=queue_name, durable=True)
            # Set QoS for each queue
            # The QoS level controls the # of messages
            # that can be in-flight (unacknowledged by the consumer)
            # at any given time.
            # Set the prefetch count to one to limit the number of messages
            # being consumed and processed concurrently.
            channel.basic_qos(prefetch_count=1)
            # Configure the channel to consume messages from each queue
            channel.basic_consume(queue=queue_name, on_message_callback=callback)

        # print a message to the console for the user
        logger.info(" [*] Waiting for stock price messages. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logger.error("ERROR: something went wrong.")
        logger.error(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.error(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logger.info("Closing connection. Goodbye.")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main()