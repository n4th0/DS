
import socket
import sys

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"


aux = True

def send(msg):

    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
########## MAIN ##########


# print("****** WELCOME TO OUR BRILLIANT SD UA CURSO 2020/2021 SOCKET CLIENT ****")

if  (len(sys.argv) == 3):
    SERVER = sys.argv[1] # "127.0.0.1" "localhost"
    PORT = int(sys.argv[2])
    ADDR = (SERVER, PORT)
    
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client.connect(ADDR)
    except ConnectionRefusedError as e:
        print("ERROR: not server found")
        exit(1)

    print (f"Establecida conexión en [{ADDR}]")

    # msg=sys.argv[3]
    msg = ""
    try:

        while msg != FIN :
            msg=input()
            print("Envio al servidor: ", msg)
            send(msg)
            print("Recibo del Servidor: ", client.recv(2048).decode(FORMAT))

    except KeyboardInterrupt:
        print("\nProgram interrupted by user. Exiting cleanly...")

    except (BrokenPipeError, ConnectionResetError):
        # TODO: put a better log file
        print("ERROR: couldn't reach the server, shutting down client")
        aux = False


    print ("SE ACABO LO QUE SE DABA")
    if aux:
        print("Envio al servidor: ", FIN)
        send(FIN)
    client.close()
else:
    print ("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <Texto Bienvenida>")


