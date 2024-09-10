import socket
import time
import random
import json
from constants import *

class Cliente:
    def __init__(self,host, porta, id=1) -> None:
        self.id = id
        self.host = host
        self.porta = porta
        self.num_requisicoes = random.randint(10, 50)  # Numero aleatorio de requisicoes
    
    def enviar_requisicao(self, connection):
        # Obtendo o timestamp e enviando para o no
        timestamp = time.time()
        mensagem = {"timestamp": timestamp}
        mensagem = json.dumps(mensagem)
        connection.sendall(mensagem.encode())

    def esperar_resposta(self, connection):
        # Esperando a resposta do no e exibindo-a
        resposta = connection.recv(BUFFER_SIZE).decode()
        resposta = json.loads(resposta)
        print(resposta)

    def ficar_ocioso(self):
        tempo = random.uniform(1, 5)
        time.sleep(tempo)

    def __call__(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
            connection.connect((self.host, self.porta))
            for _ in range(self.num_requisicoes):
                self.enviar_requisicao(connection)
                self.esperar_resposta(connection)
                self.ficar_ocioso()


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Uso: python3 client.py <host> <porta_no>")
        sys.exit(1)

    host = sys.argv[1]
    porta_no = int(sys.argv[2])
    
    cliente = Cliente(
        host = host,
        porta = porta_no
    )

    cliente()
