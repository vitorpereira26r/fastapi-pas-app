class User:
    def __init__(self, id, name, nick, username, cpf):
        self.id = id
        self.name = name
        self.nick = nick
        self.username = username
        self.cpf = cpf


class UserInfo:
    def __init__(self, name, user, idTerminal, cpf, address, cep):
        self.name = name
        self.user = user
        self.idTerminal = idTerminal
        self.cpf = cpf
        self.address = address
        self.cep = cep


class PaidBilling:
    def __init__(self, ourNumber, idTerminal, vencDate, value, status, dataQuit, paidValue, feesValue):
        self.ourNumber = ourNumber
        self.idTerminal = idTerminal
        self.vencDate = vencDate
        self.value = value
        self.status = status
        self.dataQuit = dataQuit
        self.paidValue = paidValue
        self.feesValue = feesValue


class OpenBilling:
    def __init__(self, id, idTerminal, dateVenc, value, status, code):
        self.id = id
        self.idTerminal = idTerminal
        self.dateVenc = dateVenc
        self.status = status
        self.code = code
        self.value = value


class PhoneLine:
    def __init__(self, idTerminal, idOperator, operator):
        self.idTerminal = idTerminal
        self.idOperator = idOperator
        self.operator = operator


class Service:
    def __init__(self, description, value):
        self.description = description
        self.value = value


class PhoneServices:
    def __init__(self, internetConsume, otherServices):
        self.internetConsume = internetConsume
        self.otherServices = otherServices
