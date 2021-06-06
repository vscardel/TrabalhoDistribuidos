from functools import reduce
from dateutil import parser
import threading
import datetime
from time import ctime
import ntplib
import socket
import time

#descobrir como funciona a sincronização exatamente
#criar uma thread que envia o valor de ntp de tempos em tempos para os quatro processos
#os processos vao ficar rodando berkeley atualizando seus relogios internos da seguinte maneira:
    #toda vez que eles trocarem figurinha, o relogio interno eh a media dos relogios
    #toda vez que um processo recebe o valor do npt do processo mágico ele compara o valor do ntp com o relogio
    #e ajusta caso seja necessário
  
 
#parametro K
K = 1

# datastructure used to store client address and clock data
relogios = {}

port_address = [

  8080,
  8081,
  8082,
  8083,

]

client_clocks = {
    '0': None,
    '1': None,
    '2': None,
    '3': None
}

sockets_clientes_dict = {
    0:None,
    1:None,
    2:None,
    3:None
}

ntp_start = 0


clientToMaster = {
    
}

master_port_number = {
    '8080': '0',
    '8081': '1',
    '8082': '2',
    '8083': '3'
}


''' nested thread function used to receive 
    clock time from a connected client '''


def send_ntp(socket_list):

    while True:

        print('Ciclo de envio do NTP')


        try:
            c = ntplib.NTPClient()
            response = c.request('south-america.pool.ntp.org',version=3)
            date_time = ctime(response.tx_time)
        except:
            print('problema na requisicao do ntp')
      

        for sk in socket_list:
            port_number = sk.getsockname()[1]
            client_socket = socket.socket()  
            client_socket.connect(('127.0.0.1', port_number))
            send_string = 'NTP*'+ str(date_time)
            client_socket.send(send_string.encode())
            client_socket.shutdown(socket.SHUT_RDWR)
            client_socket.close()

        time.sleep(5)



#modificar
def startRecieveingClockTime(connector, address):
  
    global ntp_start
    global K

    while True:
        # recieve clock time
        rcv_string = connector.recv(1024).decode()
        check_string = rcv_string.split('*')

        if rcv_string:

            if check_string[0] == 'NTP':
                #atualiza os relogios com o valor inicial do ntp
                if ntp_start < 4:
                    my_process_number = master_port_number[str(connector.getsockname()[1])]
                    relogios[my_process_number] = rcv_string.split('*')[1]
                    ntp_start += 1
                else:
                    ntp_time_string = rcv_string.split('*')[1]
                    ntp_time = parser.parse(ntp_time_string)
                    my_process_number = master_port_number[str(connector.getsockname()[1])]
                    my_time = relogios[my_process_number]

                    if abs(my_time.timestamp()-ntp_time.timestamp()) > K:
                        print('corrigido')
                        relogios[my_process_number] = ntp_time_string

            else:
                clock_time_string = rcv_string.split('*')[0]
                sender_process_number = rcv_string.split('*')[1]
                clock_time = parser.parse(clock_time_string)

                my_process_number = master_port_number[str(connector.getsockname()[1])]
                client_process_number = clientToMaster[connector.getpeername()[1]]

                if not client_clocks[my_process_number]:
                    my_clocks = client_clocks[my_process_number] = {}
                    my_clocks[client_process_number] = clock_time
                else:
                    my_clocks = client_clocks[my_process_number]
                    my_clocks[client_process_number] = clock_time
                    #executar o berkeley
                    if len(my_clocks) == 3:
                        my_clocks = client_clocks[my_process_number]
                        all_clocks = []
                        for key in my_clocks:
                            date = my_clocks[key]
                            seconds = date.timestamp()
                            all_clocks.append(seconds)

                        all_clocks.append(datetime.datetime.now().timestamp())
                        all_clocks.sort()

                        middle_clocks = all_clocks[1:len(all_clocks)-1]
                        summ = sum(middle_clocks)
                        media_segundos = summ/2
                        data_media = datetime.datetime.fromtimestamp(media_segundos)
                        relogios[my_process_number] = data_media


                print('Relógio do processo ' + master_port_number[str(connector.getsockname()[1])] + ' recebeu o horario ' + str(clock_time) + ' do processo ' + str(clientToMaster[connector.getpeername()[1]]) )

        time.sleep(5)
  
  
''' master thread function used to open portal for 
    accepting clients over given port '''
def acceptConnection(master_server):
      
    # fetch clock time at slaves / clients
    while True:
        # accepting a client / slave clock client
        master_slave_connector, addr = master_server.accept()
        slave_address = str(addr[0]) + ":" + str(addr[1])
  
        print(slave_address + " got connected successfully")
  
        current_thread = threading.Thread(
                         target = startRecieveingClockTime,
                         args = (master_slave_connector,
                                           slave_address, ))
        current_thread.start()

def requestConnection(sockets_clientes_dict):

    while True:

        for processo in sockets_clientes_dict:
            #processos clientes
            clientes = sockets_clientes_dict[processo]

            #ideia: so mandar o ntp em vez de datetime
            for socket_cliente in clientes:
                #mandar uma string que tenha o numero do processo junto
                #usando '*' como separador
                send_string = str(datetime.datetime.now()) + '*' + str(processo)
                socket_cliente.send(send_string.encode())
                print('tempo recente do processo ' + str(processo) + " enviado com sucesso\n")
  
        time.sleep(5)
  
  
# function used to initiate the Clock Server / Master Node
def initiateClockServer(socket_list):

    ntp_thread = threading.Thread(
                      target = send_ntp,
                      args = (socket_list,) )

    ntp_thread.start()

    while ntp_start < 4:
        pass

    print(relogios)

    for i in range(len(socket_list)):

        sockets_clientes_i = []

        for j in range(len(socket_list)):

            if i != j:
                #numero da porta dos sockets dos outros processos
                port_number = socket_list[j].getsockname()[1]
                client_socket = socket.socket()  
                client_socket.connect(('127.0.0.1', port_number))
                sockets_clientes_i.append(client_socket)
                porta_cliente = client_socket.getsockname()[1]
                clientToMaster[porta_cliente] = i


        sockets_clientes_dict[i] = sockets_clientes_i


    #passa os sockets servidores para se conectar um com o outro
    request_thread = threading.Thread(
                      target = requestConnection,
                      args = (sockets_clientes_dict,) )

    request_thread.start()

  
  
  
# Driver function
if __name__ == '__main__':
  
    # Trigger the Clock Server
    socket_list = []

    for port in port_address:

        #cria socket para escuta
        socket_escuta = socket.socket()
        socket_escuta.setsockopt(socket.SOL_SOCKET,
                                   socket.SO_REUSEADDR, 1)

        print("Socket de escuta na porta " + str(port) + 
            " criado com sucesso")
        print()


        socket_escuta.bind(('', port))
      

        #socket de escuta criado habilitado para receber requisições
        socket_escuta.listen(10)
        print("Relógio do servidor escutando na porta " + str(port))
        print()


        #socket de escuta habilitado para aceitar requisiçoes
        print("Escuta threads")
        print()
        master_thread = threading.Thread(
                            target = acceptConnection,
                            args = (socket_escuta, ))

        master_thread.start()

        socket_list.append(socket_escuta)

        #cada socket em socket_list esta habilitado a aceitar as requisicoes,
        #e ao aceitar, receber os tempos de relogio e guardar os dados em
        #client_data


    initiateClockServer(socket_list)