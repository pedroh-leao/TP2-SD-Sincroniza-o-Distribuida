import socket
import time
import random
import json
from constants import *

class Cliente:
    def __init__(self,host, port, id=1) -> None:
        self.id = id
        self.host = host
        self.port = port
        self.num_requisicoes = random.randint(10, 50)  # Número aleatório de requisições
    
    def enviar_requisicao(self, connection):
        # Obtendo o timestamp e enviando para o nó
        timestamp = time.time()
        mensagem = {"timestamp": timestamp}
        mensagem = json.dumps(mensagem)
        connection.sendall(mensagem.encode())

    def esperar_resposta(self, connection):
        # Esperando a resposta do nó e exibindo-a
        resposta = connection.recv(BUFFER_SIZE).decode()
        resposta = json.loads(resposta)
        print(resposta)

    def ficar_ocioso(self):
        tempo = random.uniform(1, 5)
        time.sleep(tempo)

    def __call__(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
            connection.connect((self.host, self.port))
            for _ in range(self.num_requisicoes):
                self.enviar_requisicao(connection)
                self.esperar_resposta(connection)
                self.ficar_ocioso()


if __name__ == "__main__":
    cliente = Cliente(
        host = '127.0.0.1',
        port = 12345 
    )

    cliente()
