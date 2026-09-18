import socket 
import threading


HEADER = 64
PORT = 5050
SERVER =  socket.gethostbyname(socket.gethostname()) # TODO: change me if i want to be in local host ""
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 2


def manage_actions(input_str: str):

    parts = input_str.strip().split()

    if parts[0] in ("+", "-", "*", "/"):
        # Validate input length
        if len(parts) != 3:
            return "ERROR: It must contain exactly 3 tokens "
            # raise ValueError("Input string must contain exactly 3 tokens: 'command arg1 arg2'")

        command, raw_arg1, raw_arg2 = parts

        # Convert arguments to numbers
        try:
            arg1 = float(raw_arg1)
            arg2 = float(raw_arg2)
        except ValueError:
            return "ERROR: arg1 and arg2 must be valid numbers. "
            # raise ValueError("arg1 and arg2 must be valid numbers.")

        # Perform the operation
        match command:
            case "+":
                return str(arg1 + arg2)
            case "-":
                return str(arg1 - arg2)
            case "*":
                return str(arg1 * arg2)
            case "/":
                if arg2 == 0:
                    return "ERROR: Cannot divide by zero... "
                return str(arg1 / arg2)


    def invertir_cadenas(lista: list[str]) -> list[str]:
        return [texto[::-1] for texto in lista]

    if parts[0] == "REVERSE":
        parts.pop(0)
        parts = invertir_cadenas(parts)
        return " ".join(parts)



    return "ERROR: parsing"




def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")

    connected = True
    while connected:
        try: 
            msg_bytes = conn.recv(HEADER)
            if not msg_bytes:
                break

            msg_length = msg_bytes.decode(FORMAT)
            if msg_length:
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(FORMAT)
                if msg == FIN:
                    connected = not connected

                # msg = action arguments

                print(f" He recibido del cliente [{addr}] el comando: {msg}")
                s = manage_actions(msg)

                conn.send(f"Tu resultado es: {s} ".encode(FORMAT))


        except (BrokenPipeError, ConnectionResetError):
            break

    print("ADIOS. TE ESPERO EN OTRA OCASION")
    conn.close()
    
        

def start():
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {SERVER}")
    CONEX_ACTIVAS = threading.active_count()-1
    print(CONEX_ACTIVAS)
    try: 
        while True:
            conn, addr = server.accept()
            CONEX_ACTIVAS = threading.active_count()
            if (CONEX_ACTIVAS <= MAX_CONEXIONES): 
                thread = threading.Thread(target=handle_client, args=(conn, addr))
                thread.start()
                print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
                print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES-CONEX_ACTIVAS)
            else:
                print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
                conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
                conn.close()
                CONEX_ACTUALES = threading.active_count()-1
            
    except KeyboardInterrupt:
        # TODO: enviar a los clientes que el servidor ha caido
        print("good bye")

######################### MAIN ##########################


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

print("[STARTING] Servidor inicializándose...")

start()

