import socket
import time
import random
import json
from constants import *
import sys

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
        print(f"Cliente {self.id} enviando requisição para o nó com timestamp {timestamp}")
        try:
            connection.sendall(mensagem.encode())
        except (ConnectionResetError, ConnectionAbortedError):
            print("Falha ao enviar a requisição. Nó desconectado. Desligando...")
            sys.exit()

    def esperar_resposta(self, connection):
        # Esperando a resposta do no e exibindo-a
        resposta = ''
        try:
            while resposta == '':
                resposta = connection.recv(BUFFER_SIZE).decode()
                if resposta:
                    resposta = json.loads(resposta)
                    print(f"Cliente {self.id} recebeu {resposta["status"]} do host {self.host}")


        except socket.timeout:
            print(f"Falha após o pedido do cliente {self.id} durante a espera de um retorno do host {self.host}. Desligando...")
            sys.exit()

    def ficar_ocioso(self):
        tempo = random.uniform(1, 5)
        print(f"Cliente {self.id} em espera por {tempo} segundos")
        time.sleep(tempo)

    def __call__(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
            connection.connect((self.host, self.porta))
            for _ in range(self.num_requisicoes):
                self.enviar_requisicao(connection)
                self.esperar_resposta(connection)
                self.ficar_ocioso()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Uso: python3 client.py <id_cliente> <host> <porta_no>")
        sys.exit(1)

    id_cliente = sys.argv[1]
    host = sys.argv[2]
    porta_no = int(sys.argv[3])
    
    cliente = Cliente(
        id = id_cliente,
        host = host,
        porta = porta_no
    )

    cliente()
