import socket
import time
import random
import json
import threading
from constants import *
import sys

maximo_inteiro = sys.maxsize

class No:
    incremento_proxima_proxima = 1000

    def __init__(self, id_no, num_de_nos, host, porta_cliente, porta_no_atual, proximo_host, proxima_porta_no, proximo_proximo_host) -> None:
        self.id_no = id_no
        self.num_de_nos = num_de_nos
        self.host = host
        self.porta_cliente = porta_cliente   # Porta com a qual o cliente vai se conectar
        self.porta_no_atual = porta_no_atual # Porta com a qual o no anterior vai se conectar
        self.proximo_host = proximo_host
        self.proxima_porta_no = proxima_porta_no
        self.proximo_proximo_host = proximo_proximo_host

        self.timestamp_cliente = None
        self.token = [None] * num_de_nos  # Inicializa o vetor vazio

        self.clienteConectado = False # Flag para sabermos se tem um cliente coonectado a esse no
        
        # Iniciando sockets para a comunicacao com o cliente, o no anterior e o proximo no
        self.cliente_socket     = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Escuta e fala
        self.no_anterior_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Somente escuta
        self.no_proximo_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Somente fala

        self.esperar_resposta = threading.Event()
        self.mutex = threading.Lock()
        
        self.print_na_tela = ""

        # Tolerancia a falha
        self.no_proximo_proximo_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.no_anterior_anterior_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        self.no_anterior_conectado = False
        self.no_anterior_anterior_conectado = False
        self.no_proximo_conectado = False
        self.no_proximo_proximo_conectado = False

    # Roda em uma thread separada
    def escutar_cliente(self):
        # Fazendo o binding
        self.cliente_socket.bind((self.host, self.porta_cliente))
        self.cliente_socket.listen()

        conn, addr = self.cliente_socket.accept()
    
        with conn:
            with self.mutex:
                self.clienteConectado = True
                print(f"Nó {self.id_no} conectado com o cliente {addr}")

            while True:
                dados = conn.recv(BUFFER_SIZE)
                if not dados:
                    break
                mensagem = json.loads(dados.decode())
                timestamp = mensagem.get('timestamp')
                #print(f"Nó {self.id_no} recebeu request do cliente com timestamp {timestamp}")
                self.timestamp_cliente = timestamp

                self.esperar_resposta.wait() # Bloqueando a thread, esperando o no sair da regiao critica

                conn.sendall(json.dumps({"status": "commited"}).encode())                    

    # Realiza o bind para permitir que o no anterior possa se conectar
    def bind_para_no_anterior(self):
        self.no_anterior_socket.bind((self.host, self.porta_no_atual))
        self.no_anterior_socket.listen()

        self.no_anterior_anterior_socket.bind((self.host, self.porta_no_atual + No.incremento_proxima_proxima))
        self.no_anterior_anterior_socket.listen()

    # Inicia a conexao com o no proximo do anel
    def conectar_ao_no_proximo(self):
        sucesso_conexao = self.no_proximo_socket.connect_ex(
            (self.proximo_host, self.proxima_porta_no)
        )
        self.no_proximo_conectado = (sucesso_conexao == 0)

        sucesso_conexao = self.no_proximo_proximo_socket.connect_ex(
            (self.proximo_host, self.proxima_porta_no + No.incremento_proxima_proxima)
        )
        self.no_proximo_proximo_conectado = (sucesso_conexao == 0)

    def aceitar_conexoes_anterior(self):
        self.conn_anterior, _ = self.no_anterior_socket.accept()
        self.no_anterior_conectado = True

    def aceitar_conexoes_anterior_anterior(self):
        self.conn_anterior_anterior, _ = self.no_anterior_anterior_socket.accept()
        self.no_anterior_anterior_conectado = True

    def aceitar_conexoes(self):
        anterior_th = threading.Thread(target=self.aceitar_conexoes_anterior)
        anterior_th.start()
        anterior_anterior_th = threading.Thread(target=self.aceitar_conexoes_anterior_anterior)
        anterior_anterior_th.start()

        anterior_th.join()
        anterior_anterior_th.join()

    # Escreve no token o timestamp do cliente, caso tenha informacoes, ou nulo, caso contrario
    def escrever_no_token(self):
        self.token[self.id_no] = self.timestamp_cliente
        # Exibindo na tela caso o novo valor para exibir seja diferente do anterior
        exibir = f"Nó {self.id_no} - Estado atual do vetor de tokens: {self.token}"
        if exibir != self.print_na_tela: 
            #print(exibir)
            self.print_na_tela = exibir

    # Envia o token para o proximo no do anel
    def enviar_para_proximo(self, vetor):
        if self.no_proximo_conectado:
            try:
                self.no_proximo_socket.sendall(json.dumps(vetor).encode())
            except (ConnectionResetError, ConnectionAbortedError):
                self.no_proximo_conectado = False
                self.token[(self.id_no + 1) % self.num_de_nos] = None

        if self.no_proximo_proximo_conectado:
            try:
                self.no_proximo_proximo_socket.sendall(json.dumps(vetor).encode())
            except (ConnectionResetError, ConnectionAbortedError):
                self.no_proximo_proximo_conectado = False            

    # Fica esperando o no anterior enviar o token e atualiza o local com os novos valores
    #def esperar_token_anterior(self):

    def esperar_token(self):   
        token_anterior = None # token recebido do no anterior
        token_anterior_anterior = None # "backup" do token recebido do no anterior do anterior, para caso o no anterior caia
        def anterior():
            nonlocal token_anterior
            try:
                dados = self.conn_anterior.recv(BUFFER_SIZE)
                if dados:
                    token_anterior = json.loads(dados.decode()) 
            except socket.timeout:
                self.no_anterior_conectado = False
                token_anterior = None
            except json.JSONDecodeError:
                pass

        def anterior_anterior():
            nonlocal token_anterior_anterior
            try:
                dados = self.conn_anterior_anterior.recv(BUFFER_SIZE)
                if dados:
                    token_anterior_anterior = json.loads(dados.decode()) 
            except socket.timeout:
                self.no_anterior_anterior_conectado = False
                token_anterior_anterior = None
            except json.JSONDecodeError:
                pass

        threads = []
        if self.no_anterior_conectado:
            t = threading.Thread(target=anterior)
            t.start()
            threads.append(t)
        if self.no_anterior_anterior_conectado:
            t = threading.Thread(target=anterior_anterior)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        if token_anterior is not None:
            self.token = token_anterior
        elif token_anterior_anterior is not None:
            self.token = token_anterior_anterior


    # Verifica se pode ou nao acessar a regiao critica
    def verificar_regiao_critica(self):
        tokens_copia = [x if x is not None else maximo_inteiro for x in self.token]

        menor_timestamp = min(tokens_copia)

        return self.timestamp_cliente == menor_timestamp and menor_timestamp != maximo_inteiro

    # Entra na regiao critica e executa o que deve executar. No caso, um sleep
    def entrar_regiao_critica(self):
        print(f"Nó {self.id_no} entrando na seção crítica...")
        tempo = random.uniform(0.2, 1)
        time.sleep(tempo)
        print(f"Nó {self.id_no} saiu da seção crítica.")

    def sair_da_regiao_critica(self):
        self.token[self.id_no] = None  # Limpa a posicao do vetor apos sair da regiao critica
        self.timestamp_cliente = None  # Remove o timestamp do cliente

        with self.mutex:
            if self.clienteConectado == True:
                self.esperar_resposta.set()    # Desbloqueando a thread do cliente para enviar o commit 

    def executar_no(self):
        self.bind_para_no_anterior()
        # Garantindo que o nó anterior já realizou o bind e eu consigo conectar, uma vez que o compose nao garante a inicializacao sequencial
        time.sleep(2)
        self.conectar_ao_no_proximo()
        self.aceitar_conexoes()

        # O primeiro no inicia o processo de escrever no vetor (token) vazio do anel e passar ao proximo no
        if self.id_no == 0:
            self.escrever_no_token() # Escreve no vetor
            self.enviar_para_proximo(self.token)

        while True:
            self.esperar_token() # Espera o vetor (token) do no anterior

            if self.verificar_regiao_critica():
                self.entrar_regiao_critica()
                self.sair_da_regiao_critica()

            else:
                self.escrever_no_token() # Escreve no vetor
            
            self.enviar_para_proximo(self.token) # Envia o vetor para o proximo no


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 9:
        print("Uso: python3 no.py <id_no> <num_de_nos> <host> <porta_cliente> <porta_no_atual> <proximo_host> <proximo_port>")
        sys.exit(1)

    id_no = int(sys.argv[1])
    num_de_nos = int(sys.argv[2])
    host = sys.argv[3]
    porta_cliente = int(sys.argv[4])  # Porta para clientes
    porta_no_atual = int(sys.argv[5])  # Porta para comunicação entre nos
    proximo_host = sys.argv[6]
    proximo_port = int(sys.argv[7])
    proximo_proximo_host = sys.argv[8]

    no = No(id_no, num_de_nos, host, porta_cliente, porta_no_atual, proximo_host, proximo_port, proximo_proximo_host)
    
    # Inicia a thread para escutar o cliente
    threading.Thread(target=no.escutar_cliente, daemon=True).start()
    
    # Executa o loop principal do no
    no.executar_no()
    