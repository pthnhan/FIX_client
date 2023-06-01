import quickfix as fix


class FixClient(fix.Application):
    """
    This class is used to handle all the callbacks from the fix engine
    """

    def onCreate(self, sessionID):
        """
        This method is called when a new session is created
        :param sessionID: sessionID
        :return:
        """
        self.sessionID = sessionID

    def onLogon(self, sessionID):
        """
        This method is called when the session is logged on
        :param sessionID: sessionID
        :return:
        """
        print(f"Logon - session: {sessionID}")

    def onLogout(self, sessionID):
        """
        This method is called when the session is logged out
        :param sessionID: sessionID
        :return:
        """
        print(f"Logout - session: {sessionID}")

    def toAdmin(self, message, sessionID):
        """
        This method is called when the application sends a message to the counter party
        :param message: message
        :param sessionID: sessionID
        :return:
        """
        msg_type = fix.MsgType()
        message.getHeader().getField(msg_type)
        if msg_type.getValue() == fix.MsgType_Logon:
            print("Logon Message:", message)
    
    def fromAdmin(self, message, sessionID):
        """
        This method is called when a message is received from the counter party
        :param message: message
        :param sessionID: sessionID
        :return:
        """
        print("Admin Message:", message)
        msg_type = fix.MsgType()
        message.getHeader().getField(msg_type)
        if msg_type.getValue() == fix.MsgType_Reject:
            print("Admin Reject Message:", message)
        elif msg_type.getValue() == fix.MsgType_OrderCancelReject:
            print("Admin Order Cancel Reject Message:", message)
        elif msg_type.getValue() == fix.MsgType_ExecutionReport:
            print("Admin Execution Report Message:", message)

    def toApp(self, message, sessionID):
        """
        This method is called when the application sends a message to the counter party
        :param message: message
        :param sessionID: sessionID
        :return:
        """
        msg_type = fix.MsgType()
        message.getHeader().getField(msg_type)
        if msg_type.getValue() == fix.MsgType_NewOrderSingle:
            print("New Order Message:", message)
        elif msg_type.getValue() == fix.MsgType_OrderCancelRequest:
            print("Order Cancel Message:", message)

    def fromApp(self, message, sessionID):
        """
        This method is called when a message is received from the counter party
        :param message: message
        :param sessionID: sessionID
        :return:
        """
        print("Incoming Application Message:", message)
        msg_type = fix.MsgType()
        message.getHeader().getField(msg_type)
        if msg_type.getValue() == fix.MsgType_Reject:
            print("New Order Reject Message:", message)
        elif msg_type.getValue() == fix.MsgType_OrderCancelReject:
            print("Order Cancel Reject Message:", message)
        elif msg_type.getValue() == fix.MsgType_ExecutionReport:
            print("Execution Report Message:", message)

