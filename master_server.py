from functools import reduce
from dateutil import parser
import threading
import datetime
from time import ctime
import ntplib
import socket
import time
 
# Máximo desvio de tempo com relação ao relógio retornado pelo servidor NTP
K = 1

# Relógios dos processos
relogios = {}

# Porta dos sockets de escuta de processos
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

# Lista de sockets criados para se conectar a outros processos
# Por processo
sockets_clientes_dict = {
    0:None,
    1:None,
    2:None,
    3:None
}

# Número de processos inicializados com o relógio NTP
ntp_start = 0

# Mapear socket cliente para o processo que o criou
clientToMaster = {
    
}

# Dicionário que mapeia a porta do processo ao seu número
master_port_number = {
    '8080': '0',
    '8081': '1',
    '8082': '2',
    '8083': '3'
}


def send_ntp(socket_list):

    while True:

        print('Ciclo de envio do NTP')


        try:
            c = ntplib.NTPClient()
            response = c.request('south-america.pool.ntp.org',version=3)
            date_time = ctime(response.tx_time)
        except:
            print('Problema na requisicao do ntp')
      

        for sk in socket_list:
            port_number = sk.getsockname()[1]
            client_socket = socket.socket()  
            client_socket.connect(('127.0.0.1', port_number))
            send_string = 'NTP*'+ str(date_time)
            client_socket.send(send_string.encode())
            client_socket.shutdown(socket.SHUT_RDWR)
            client_socket.close()

        time.sleep(5)
  

########################### FUNÇÕES CLIENTE ###########################

'''Requisitar conexão e enviar horário recente'''
def requestConnection(sockets_clientes_dict):

    while True:

        for processo in sockets_clientes_dict:
            # Sockets clientes
            clientes = sockets_clientes_dict[processo]

            for socket_cliente in clientes:
                # Formato da string: "horário*número_do_processo_mestre"
                send_string = str(datetime.datetime.now()) + '*' + str(processo)
                socket_cliente.send(send_string.encode())
                
                master_process_number = master_port_number[socket_cliente.getpeername()[1]]

                print("[PROCESSO " + str(processo) + "] Enviou horário com sucesso para o processo " + master_process_number)
  
        time.sleep(5)
  
  
''' Inicialização NTP.
    Se conectar aos outros processos '''
def initiateClockServer(socket_list):

    # Sincronização externa
    ntp_thread = threading.Thread(
                      target = send_ntp,
                      args = (socket_list,) )
    ntp_thread.start()

    # Enquanto nem todos os processos foram inicializados com o valor NTP
    while ntp_start < 4:
        pass

    # Após todos os processos terem seus relógios inicializados.
    # Criar sockets para se conectar aos outros processos.
    for i in range(len(socket_list)):

        sockets_clientes_i = []

        for j in range(len(socket_list)):

            if i != j:
                
                port_number = socket_list[j].getsockname()[1]

                client_socket = socket.socket()

                porta_cliente = client_socket.getsockname()[1]
                clientToMaster[porta_cliente] = i
                
                client_socket.connect(('127.0.0.1', port_number))

                sockets_clientes_i.append(client_socket)

        sockets_clientes_dict[i] = sockets_clientes_i


    # Requisitar conexões
    request_thread = threading.Thread(
                      target = requestConnection,
                      args = (sockets_clientes_dict,) )

    request_thread.start()


########################### FUNÇÕES MASTER ###########################


''' Ao receber um relógio. 
    Verificar se é de um servidor NTP e setar o próprio
    relógio caso ainda não tenha sido inicializado ou caso
    o desvio supere K.
    Verificar se é de um cliente e aplicar Berkeley '''
def startRecieveingClockTime(connector, address):
  
    global ntp_start
    global K

    while True:
        
        rcv_string = connector.recv(1024).decode()
        check_string = rcv_string.split('*')

        # Se um valor de horário chegou
        if rcv_string:

            # Se é um valor de relógio retornado pelo NTP
            if check_string[0] == 'NTP':
                
                # Se nem todos os processos foram inicializados
                # Inicialize - os com o valor recebido
                if ntp_start < 4:
                    my_process_number = master_port_number[str(connector.getsockname()[1])]
                    relogios[my_process_number] = rcv_string.split('*')[1]
                    ntp_start += 1
                
                # Se todos os processos foram inicializados
                # Verifica se o relógio do processo se desvia mais que
                # K do valor do relógio NTP
                else:
                	# relógio NTP
                    ntp_time_string = rcv_string.split('*')[1]
                    ntp_time = parser.parse(ntp_time_string)
                    
                    # relógio do processo
                    my_process_number = master_port_number[str(connector.getsockname()[1])]
                    my_time = relogios[my_process_number]

                    if abs(my_time.timestamp()-ntp_time.timestamp()) > K:
                        relogios[my_process_number] = ntp_time_string
                        print("[PROCESSO " + my_process_number + "] Relógio corrigido com valor NTP")

            # Se o relógio que chegou foi enviado por um dos clientes
            else:
                clock_time_string = rcv_string.split('*')[0]
                sender_process_number = rcv_string.split('*')[1]
                clock_time = parser.parse(clock_time_string)

                my_process_number = master_port_number[str(connector.getsockname()[1])]
                client_process_number = clientToMaster[connector.getpeername()[1]]

                print("[PROCESSO " + my_process_number + "] Recebeu o valor de relógio " + clock_time_string + " do processo " + client_process_number)

                # Se o dicionário de relógios ainda não foi criado
                if not client_clocks[my_process_number]:
                    my_clocks = client_clocks[my_process_number] = {}
                    my_clocks[client_process_number] = clock_time
                
                # Se o dicionário de relógios já foi criado
                else:
                    my_clocks = client_clocks[my_process_number]
                    my_clocks[client_process_number] = clock_time

                    # Se já há informações de todos os processos.
                    # BERKELEY
                    if len(my_clocks) == 3:
                        my_clocks = client_clocks[my_process_number]
                        all_clocks = []

                        # Pegar relógios dos clientes
                        for key in my_clocks:
                            date = my_clocks[key]
                            seconds = date.timestamp()
                            all_clocks.append(seconds)

                        # Adicionar próprio relógio
                        all_clocks.append(datetime.datetime.now().timestamp())
                        all_clocks.sort()

                        # Berkeley
                        middle_clocks = all_clocks[1:len(all_clocks)-1]
                        summ = sum(middle_clocks)
                        media_segundos = summ/2
                        data_media = datetime.datetime.fromtimestamp(media_segundos)
                        relogios[my_process_number] = data_media

                        print("[PROCESSO " + my_process_number + "] Retorno da sincronização interna: " + str(data_media))


        time.sleep(5)



  
''' Função que aguarda a requisição de conexão 
    dos outros processos. Cada processo possui uma
    thread master que aguarda conexões '''
def acceptConnection(master_server):
      
    while True:
        
        master_slave_connector, addr = master_server.accept()
        slave_address = str(addr[0]) + ":" + str(addr[1])

        my_process_number = master_port_number[str(master_slave_connector.getsockname()[1])]
        client_process_number = clientToMaster[master_slave_connector.getpeername()[1]]

        print("[PROCESSO " + my_process_number + "] Processo " + client_process_number + " se conectou")
        print()

        current_thread = threading.Thread(
                         target = startRecieveingClockTime,
                         args = (master_slave_connector,
                                           slave_address, ))
        current_thread.start()


  
if __name__ == '__main__':
  
    socket_list = []

    # Para cada processo:
    for port in port_address:

        my_process_number = master_port_number[str(port)]

        # Cria socket para escuta
        socket_escuta = socket.socket()
        socket_escuta.setsockopt(socket.SOL_SOCKET,
                                   socket.SO_REUSEADDR, 1)
        socket_escuta.bind(('', port))

        print("[PROCESSO " + master_port_number[str(port)] + "] Socket de escuta na porta " + str(port) + " criado com sucesso")
        print()

        # Socket de escuta criado e habilitado para receber requisições
        socket_escuta.listen(10)
        print("[PROCESSO " + my_process_number + "] Relógio do servidor escutando na porta " + str(port))
        print()


        # Socket de escuta habilitado para aceitar requisiçoes
        print("[PROCESSO " + my_process_number + "] Aguardando requisições")
        print()
        master_thread = threading.Thread(
                            target = acceptConnection,
                            args = (socket_escuta, ))

        master_thread.start()

        socket_list.append(socket_escuta)

        # Cada socket em socket_list esta habilitado a aceitar as requisicoes,
        # e ao aceitar, receber os tempos de relogio e guardar os dados em
        # seu dicionario de dados de cliente em client_data

    initiateClockServer(socket_list)