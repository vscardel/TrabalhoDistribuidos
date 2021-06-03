from functools import reduce
from dateutil import parser
import threading
import datetime
import socket
import time
  
  
# datastructure used to store client address and clock data
client_data = {}


port_address = [

  8080,
  8081,
  8082,
  8083,

]

''' nested thread function used to receive 
    clock time from a connected client '''
def startRecieveingClockTime(connector, address):
  
    while True:
        # recieve clock time
        clock_time_string = connector.recv(1024).decode()
        clock_time = parser.parse(clock_time_string)
        clock_time_diff = datetime.datetime.now() - \
                                                 clock_time
  
        client_data[address] = {
                       "clock_time"      : clock_time,
                       "time_difference" : clock_time_diff,
                       "connector"       : connector
                       }
  
        print("Client Data updated with: "+ str(address),
                                              end = "\n\n")
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

def requestConnection(client_socket):
    while True:
        # provide server with clock time at the client

        # ntp_client = ntplib.NTPClient()
        # ntp_response = ntp_client.request('south-america.pool.ntp.org',version=3)
        # curr_time = time.process_time_ns()
        # if abs(curr_time, ntp_response) > K:
        #   curr_time = ntp_response
        
        client_socket.send(str(
                       datetime.datetime.now()).encode())
  
        print("Recent time sent successfully",
                                          end = "\n\n")
        time.sleep(5)
  
  
# subroutine function used to fetch average clock difference
def getAverageClockDiff():
  
    current_client_data = client_data.copy()
  
    time_difference_list = list(client['time_difference'] 
                                for client_addr, client 
                                    in client_data.items())
                                     
  
    sum_of_clock_difference = sum(time_difference_list, \
                                   datetime.timedelta(0, 0))
  
    average_clock_difference = sum_of_clock_difference \
                                         / len(client_data)
  
    return  average_clock_difference
  
  
''' master sync thread function used to generate 
    cycles of clock synchronization in the network '''
def synchronizeAllClocks():
  
    while True:
  
        print("New synchroniztion cycle started.")
        print("Number of clients to be synchronized: " + \
                                     str(len(client_data)))
  
        if len(client_data) > 0:
  
            average_clock_difference = getAverageClockDiff()

            # print("#### average_clock_difference #### " + str(average_clock_difference ))
  
            for client_addr, client in client_data.items():
                try:
                    synchronized_time = \
                         datetime.datetime.now() + \
                                    average_clock_difference
  
                    client['connector'].send(str(
                               synchronized_time).encode())
  
                except Exception as e:
                    print("Something went wrong while " + \
                          "sending synchronized time " + \
                          "through " + str(client_addr))
  
        else :
            print("No client data." + \
                        " Synchronization not applicable.")
  
        print("\n\n")
  
        time.sleep(5)

# function used to initiate the Clock Server / Master Node
def initiateClockServer(socket_list):

    for skt in socket_list:

        curr_port_number = skt.getsockname()[1]

        client_socket = socket.socket()           
        client_socket.connect(('127.0.0.1', curr_port_number))

        request_thread = threading.Thread(
                            target = requestConnection,
                            args = (client_socket,) )

        request_thread.start()


    # start synchroniztion
    print("Starting synchronization parallely...\n")
    sync_thread = threading.Thread(
                          target = synchronizeAllClocks,
                          args = ())
    sync_thread.start()
  
  
  
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

    initiateClockServer(socket_list)