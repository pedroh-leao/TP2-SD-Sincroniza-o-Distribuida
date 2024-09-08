import socket
import time
import random
import json
import threading
from constants import *

class No:
    def __init__(self, id_no, num_de_nos, host, porta_cliente, porta_cluster,proximo_host, proxima_porta_no) -> None:
        self.id_no = id_no
        self.num_de_nos = num_de_nos
        self.host = host
        self.porta_cliente = porta_cliente # Porta com a qual o cliente vai se conectar
        self.porta_cluster = porta_cluster # Porta com a qual o no anterior vai se conectar
        self.proximo_host = proximo_host
        self.proxima_porta_no = proxima_porta_no

        self.timestamp_cliente 
        self.token = [None] * num_de_nos  # Inicializa o vetor vazio

        # Iniciando sockets para a comunicacao com o cliente, o no anterior e o proximo no
        self.cliente_socket     = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Escuta e fala
        self.no_anterior_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Somente escuta
        self.no_seguinte_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Somente fala


    def escutar_cliente(self):
        # Fazendo o binding
        self.cliente_socket.bind((self.host, self.porta_cliente))
        self.cliente_socket.listen()

        while True:
            conn, addr = self.cliente_socket.accept()
            with conn:
                print(f"Nó {self.id_no} conectado com o cliente {addr}")
                while True:
                    dados = conn.recv(1024)
                    if not dados:
                        break
                    mensagem = json.loads(dados.decode())
                    self.timestamp_cliente = mensagem.get('timestamp')

                    # !Ficar esperando bloqueado até que seja liberado retornar o commit

                    conn.sendall(json.dumps({"status": "commited"}).encode())                    

    #!#################################################################################################

    # Inicia a conexao com o no seguinte do anel
    def conectar_ao_no_seguinte(self):
        self.no_seguinte_socket.connect(
            (self.proximo_host, self.proxima_porta_no)
        )

    # Realiza o bind para permitir que o no anterior possa se conectar
    def bind_para_no_anterior(self):
        self.no_anterior_socket.bind((self.host, self.porta_cluster))
        self.no_anterior_socket.listen()

    # Escreve no token o timestamp do cliente, caso tenha informacoes, ou nulo, caso contrario
    def escrever_no_token(self):
        self.token[self.id_no] = self.timestamp_cliente
        print(f"Nó {self.id_no} escreveu no vetor: {self.token}")

    # Envia o token para o proximo no do anel
    def enviar_para_proximo(self, vetor):
        self.no_seguinte_socket.sendall(json.dumps(vetor).encode())

    # Fica esperando o no anterior enviar o token e atualiza o local com os novos valores
    def esperar_token(self):
        conn, _ = self.no_anterior_socket.accept()
        with conn:
            dados = conn.recv(BUFFER_SIZE)
            if dados:
                token = json.loads(dados.decode())
                self.token = token

    # Verifica se pode ou nao acessar a regiao critica
    def verificar_regiao_critica(self):
        # Filtrando todos os tokens que nao sao validos
        filtro = lambda x: x is not None
        vetor_token = list(filter(filtro, self.token))
        if len(vetor_token) == 0:
            return False
        # Verificando se o timestamp do cliente é o menor
        menor_timestamp = min(vetor_token)
        return self.timestamp_cliente == menor_timestamp

    # Entra na regiao critica e executa o que deve executar. No caso, um sleep
    def entrar_regiao_critica(self):
        print(f"Nó {self.id_no} entrando na seção crítica...")
        tempo = random.uniform(0.2, 1)
        time.sleep(tempo)
        print(f"Nó {self.id_no} saiu da seção crítica.")

    def sair_da_regiao_critica(self):
        self.token[self.id_no] = None  # Limpa a posicao do vetor apos sair da regiao critica
        self.timestamp_cliente = None  # Remove o timestamp do cliente

    def executar_no(self):
        self.bind_para_no_anterior()
        self.conectar_ao_no_seguinte()

        # O primeiro no inicia o processo de escrever no vetor (token) vazio do anel e passar ao proximo no
        if self.id_no == 0:
            self.escrever_no_token()
            self.enviar_para_proximo(self.token)

        while True:
            self.esperar_token()  # Espera o vetor (token) do no anterior
            self.escrever_no_token()  # Escreve no vetor

            if self.verificar_regiao_critica():
                self.entrar_regiao_critica()
                self.sair_da_regiao_critica()
                
            self.enviar_para_proximo(self.token)  # Envia o vetor para o proximo no

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 7:
        print("Uso: python3 no.py <id_no> <num_de_nos> <host> <porta_cliente> <porta_cluster> <proximo_host> <proximo_port>")
        sys.exit(1)

    id_no = int(sys.argv[1])
    num_de_nos = int(sys.argv[2])
    host = sys.argv[3]
    porta_cliente = int(sys.argv[4])  # Porta para clientes
    porta_cluster = int(sys.argv[5])  # Porta para comunicação entre nos
    proximo_host = sys.argv[6]
    proximo_port = int(sys.argv[7])

    no = No(id_no, num_de_nos, host, porta_cliente, porta_cluster, proximo_host, proximo_port)
    
    # Inicia a thread para escutar o cliente
    threading.Thread(target=no.escutar_cliente, daemon=True).start()
    
    # Executa o loop principal do no
    no.executar_no()