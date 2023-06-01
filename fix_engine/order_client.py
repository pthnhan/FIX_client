import quickfix as fix
from .engine import FixClient
from datetime import datetime
import time


class OrderClient:
    """
    This class is used to send order to the server
    """
    
    def __init__(self, file):
        """
        Initialize the order client
        :param file: config file
        """
        self.settings = fix.SessionSettings(file)
        self.fixclient = FixClient()
        self.storeFactory = fix.FileStoreFactory(self.settings)
        self.logFactory = fix.FileLogFactory(self.settings)
        self.initiator = fix.SocketInitiator(self.fixclient, self.storeFactory, self.settings, self.logFactory)

    def start(self):
        """
        Start the order client
        :return:
        """
        self.initiator.start()

    def stop(self):
        """
        Stop the order client
        :return:
        """
        self.initiator.stop()

    def logon(self):
        """
        Logon to the server
        :return:
        """
        logon = fix.Message()
        logon.getHeader().setField(fix.BeginString("FIX.4.2"))
        logon.getHeader().setField(fix.MsgType(fix.MsgType_Logon))
        fix.Session.sendToTarget(logon, self.fixclient.sessionID)

    async def send_order(self, order):
        """
        Send order to the server
        :param order: order
        :return:
        """
        msg_type = fix.MsgType()
        order.getHeader().getField(msg_type)
        if msg_type.getValue() == fix.MsgType_NewOrderSingle:
            print("Sending a new order:", order)
        elif msg_type.getValue() == fix.MsgType_OrderCancelRequest:
            print("Sending a cancel order:", order)
        fix.Session.sendToTarget(order, self.fixclient.sessionID)

def place_order_message(**order):
    """
    Create a new order message
    :param order: order
    :return:
    """
    place_order = fix.Message()
    place_order.getHeader().setField(fix.BeginString(fix.BeginString_FIX42))
    place_order.getHeader().setField(fix.MsgType(fix.MsgType_NewOrderSingle))
    place_order.getHeader().setField(fix.StringField(60,(datetime.utcnow ().strftime ("%Y%m%d-%H:%M:%S.%f"))[:-3]))

    place_order.setField(fix.ClOrdID(order.get('cl_ord_id')))
    place_order.setField(fix.HandlInst('1'))
    place_order.setField(fix.Symbol(order.get('symbol')))
    place_order.setField(fix.Side(order.get('side')))
    order_type = order.get('order_type')
    place_order.setField(fix.OrdType(order.get('order_type')))
    if order_type == '2':
        place_order.setField(fix.Price(order.get('price')))
    place_order.setField(fix.OrderQty(order.get('order_qty')))
    return place_order

def cancel_order_message(**order):
    """
    Create a cancel order message
    :param order: order
    :return:
    """
    cancel_order = fix.Message()
    cancel_order.getHeader().setField(fix.BeginString(fix.BeginString_FIX42))
    cancel_order.getHeader().setField(fix.MsgType(fix.MsgType_OrderCancelRequest))
    cancel_order.getHeader().setField(fix.StringField(60,(datetime.utcnow ().strftime ("%Y%m%d-%H:%M:%S.%f"))[:-3]))
    cancel_order.setField(fix.ClOrdID(str(int(time.time() * 1000))))
    cancel_order.setField(fix.OrigClOrdID(order.get('cl_ord_id')))
    cancel_order.setField(fix.Symbol(order.get('symbol')))
    cancel_order.setField(fix.Side(order.get('side')))
    cancel_order.setField(fix.OrderQty(order.get('order_qty')))
    return cancel_order