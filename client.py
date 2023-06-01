import os
import time
import random
import asyncio
from queue import Queue
from fix_engine.order_client import OrderClient, place_order_message, cancel_order_message


global orders_to_cancel
orders_to_cancel = []
order_id = 0

async def send_new_order(order_client, orders):
    """
    Send new order to the server
    :param order_client: OrderClient
    :param orders: list of orders
    :return:
    """
    end_time = time.time() + 5*60  # 5 minutes from now
    while time.time() < end_time and not orders.empty():
        order = orders.get()
        order_message = place_order_message(**order)
        await order_client.send_order(order_message)
        orders_to_cancel.append(order)
        await asyncio.sleep(random.uniform(0.1, 0.4))

async def send_cancel_order(order_client):
    """
    Send cancel order to the server
    :param order_client: OrderClient
    :return:
    """
    end_time = time.time() + 5 * 60  # 5 minutes from now
    await asyncio.sleep(10)
    while time.time() < end_time and len(orders_to_cancel) > 0:
        order = random.choice(orders_to_cancel)
        cancel_order = cancel_order_message(**order)
        await order_client.send_order(cancel_order)
        orders_to_cancel.remove(order)
        await asyncio.sleep(random.uniform(0.1, 0.4))

def gen_random_orders(num_orders = 1000):
    """
    Generate random orders
    :param num_orders: number of orders
    :return: order queue
    """
    order_queue = Queue(maxsize = num_orders)
    symbol_dict = {'AAPL': [120, 180], 'MSFT': [220, 350], 'BAC': [25, 40]}
    side_list = ['1', '2', '5']
    order_type_list = ['1', '2']
    qty_range = [10, 1000]
    while not order_queue.full():
        cl_ord_id = str(int(time.time() * 1000))
        symbol = random.choice(list(symbol_dict.keys()))
        price = round(random.uniform(symbol_dict[symbol][0], symbol_dict[symbol][1]), 2)
        side = random.choice(side_list)
        order_type = random.choice(order_type_list)
        order_qty = random.randint(qty_range[0], qty_range[1])
        order = {'cl_ord_id': cl_ord_id, 'symbol': symbol, 'side': side, 'order_type': order_type, 'price': price, 'order_qty': order_qty}
        order_queue.put(order)
        time.sleep(0.001)
    return order_queue

def run():
    """
    Run the order client
    :return:
    """
    # create folder
    try:
        if not os.path.exists("output"):
            os.makedirs("output")
    except:
        pass
    order_client = OrderClient("fix_config.cfg")
    order_client.start()
    order_client.logon()
    random_orders = gen_random_orders(num_orders=1000)
    t = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(send_new_order(order_client, random_orders), send_cancel_order(order_client)))
    loop.close()
    print(time.time() - t)
    order_client.stop()

if __name__ == "__main__":
    run()