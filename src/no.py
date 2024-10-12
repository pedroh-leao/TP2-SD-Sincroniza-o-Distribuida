import socket
import time
import random
import json
import threading
from constants import *
import sys
import os

maximo_inteiro = sys.maxsize

class EsperaToken:
    def __init__(self, conexao, anterior_conectado, token=None):
        self.token = token
        self.conexao = conexao # valor do conn obtido durante a conexao
        self.anterior_conectado = anterior_conectado # flag para indicar se esta ou nao conectado
        self.recebeu_resposta = False

# faz a comunicacao de um no do clusterSync com um no do clusterStore
class ClusterSync_ClusterStore:
    def __init__(self, host, porta):
        self.host = host  # host do no do cluster store
        self.porta = porta # porta do no do cluster store
        self.socket_cSync_cStore = None

    def iniciar_conexao(self):
        # Verifica se o socket é None, recria-o se necessário
        if self.socket_cSync_cStore is None:
            self.socket_cSync_cStore = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_cSync_cStore.connect((self.host, self.porta))


    def enviar_mensagem(self, mensagem):
        self.socket_cSync_cStore.sendall(json.dumps(mensagem).encode())

    def esperar_retorno(self):
        dados = self.socket_cSync_cStore.recv(BUFFER_SIZE)
        mensagem = json.loads(dados.decode())
        #print(mensagem["status"])

    def finalizar_conexao(self) :
        self.socket_cSync_cStore.close()
        self.socket_cSync_cStore = None

class ClusterStore:
    def __init__(self, lista_endrecos):
        self.cluster_store = [ClusterSync_ClusterStore(host, porta) for (host, porta) in lista_endrecos]
    
    def enviar_mensagem(self, mensagem):
        selecionado = random.randint(0, len(self.cluster_store) - 1)
        cluster = self.cluster_store[selecionado]

        cluster.iniciar_conexao()
        cluster.enviar_mensagem(mensagem)
        cluster.esperar_retorno()
        cluster.finalizar_conexao()


class No:
    incremento_proxima_proxima = 1000

    def __init__(self, id_no, num_de_nos, host, porta_cliente, porta_no_atual, proximo_host, proxima_porta_no, proximo_proximo_host, queda) -> None:        
        self.cluster_store = ClusterStore([("cluster0", 10000), ("cluster1", 10001), ("cluster2", 10002)])
        
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

        self.esperar_resposta = threading.Condition()
        self.mutex = threading.Lock()
        
        self.print_na_tela = ""

        # Tolerancia a falha
        self.no_proximo_proximo_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.no_anterior_anterior_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        self.no_anterior_conectado = False
        self.no_anterior_anterior_conectado = False
        self.no_proximo_conectado = False
        self.no_proximo_proximo_conectado = False

        self.queda = queda

    def encerrar_sockets(self):
        self.cliente_socket.close()
        self.no_anterior_socket.close()
        self.no_proximo_socket.close()
        self.no_proximo_proximo_socket.close()
        self.no_anterior_anterior_socket.close()

    def cair(self):
        numero = random.randint(1, 100)
        if numero > 40:
            self.encerrar_sockets()
            print("Derrubando no", self.id_no)
            os._exit(0)
            #sys.exit(1)

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
                self.timestamp_cliente = timestamp

                with self.esperar_resposta:
                    self.esperar_resposta.wait() # Bloqueando a thread, esperando o no sair da regiao critica

                conn.sendall(json.dumps({"status": "commited"}).encode())                    

                if self.queda == 1:
                    self.cair()

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

        proxima_porta = (self.id_no + 2) % self.num_de_nos + 6000 + No.incremento_proxima_proxima
        sucesso_conexao = self.no_proximo_proximo_socket.connect_ex(
            (self.proximo_proximo_host,  proxima_porta)
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


    # Escreve no token o timestamp do cliente, caso tenha informacoes, ou nulo, caso contrario
    def escrever_no_token(self):
        self.token[self.id_no] = self.timestamp_cliente
        # Exibindo na tela caso o novo valor para exibir seja diferente do anterior
        exibir = f"Nó {self.id_no} - Estado atual do vetor de tokens: {self.token}"
        if exibir != self.print_na_tela: 
            #print(exibir)
            self.print_na_tela = exibir

    # Envia o token para o proximo no do anel
    def enviar_para_proximo(self):
        if self.no_proximo_conectado:
            try:
                self.no_proximo_socket.sendall(json.dumps(self.token).encode())
            except:
                self.no_proximo_conectado = False
                self.token[(self.id_no + 1) % self.num_de_nos] = None

        if self.no_proximo_proximo_conectado:
            try:
                self.no_proximo_proximo_socket.sendall(json.dumps(self.token).encode())
            except:
                self.no_proximo_proximo_conectado = False 
                self.token[(self.id_no + 2) % self.num_de_nos] = None

    # Fica esperando o no anterior enviar o token e atualiza o local com os novos valores
    def esperar_token_anterior(self, espera: EsperaToken):
        dados = ''
        while espera.anterior_conectado: 
            try:
                dados = espera.conexao.recv(BUFFER_SIZE)
                if dados:
                    espera.token = json.loads(dados.decode())
                    espera.recebeu_resposta = True
                    return
            except:
                espera.recebeu_resposta = False
                espera.anterior_conectado = False
                return

    def esperar_token(self):
        # Informações a serem passadas por referencia
        anterior = EsperaToken(self.conn_anterior, self.no_anterior_conectado)
        anterior_anterior = EsperaToken(self.conn_anterior_anterior, self.no_anterior_anterior_conectado)

        t_anterior = threading.Thread(target=self.esperar_token_anterior, args=(anterior,))
        t_anterior_anterior = threading.Thread(target=self.esperar_token_anterior, args=(anterior_anterior,))
        
        t_anterior.start()
        t_anterior_anterior.start()
        
        t_anterior.join(5) 
        t_anterior_anterior.join(3)

        if self.rodadaPassandoToken != 1 and (self.id_no == 1 or self.id_no == 0):
            # Atualiza as flags de conexão
            self.no_anterior_conectado = anterior.anterior_conectado
            self.no_anterior_anterior_conectado = anterior_anterior.anterior_conectado

        # Verifica se recebeu o token do nó anterior
        if anterior.recebeu_resposta:
            self.token = anterior.token

        elif anterior_anterior.recebeu_resposta:
            self.token = anterior_anterior.token
            # Remove o timestamp do token anterior, pois era o menor e bloqueia o acesso a regiao critica
            self.token[(self.id_no - 1) % self.num_de_nos] = None

            
    # Verifica se pode ou nao acessar a regiao critica
    def verificar_regiao_critica(self):
        tokens_copia = [x if x is not None else maximo_inteiro for x in self.token]

        menor_timestamp = min(tokens_copia)

        retorno = self.timestamp_cliente == menor_timestamp and menor_timestamp != maximo_inteiro

        #print(retorno) 

        return retorno

    # Entra na regiao critica e executa o que deve executar. No caso, um sleep
    def entrar_regiao_critica(self):
        print(f"Nó {self.id_no} entrando na seção crítica...")

        mensagem = f"Cluster Sync id: {self.id_no} - Timestamp do cliente: {self.timestamp_cliente}"        
        self.cluster_store.enviar_mensagem(mensagem)
        
        if self.queda == 3:
            self.cair()

    def sair_da_regiao_critica(self):
        self.token[self.id_no] = None  # Limpa a posicao do vetor apos sair da regiao critica
        self.timestamp_cliente = None  # Remove o timestamp do cliente

        with self.mutex:
            if self.clienteConectado == True:
                with self.esperar_resposta:
                    self.esperar_resposta.notify()    # Desbloqueando a thread do cliente para enviar o commit 
        
        print(f"Nó {self.id_no} saiu da seção crítica.")

    def executar_no(self):
        self.rodadaPassandoToken = 1

        self.bind_para_no_anterior()
        time.sleep(1)
        threading.Thread(target=self.aceitar_conexoes).start()

        self.conectar_ao_no_proximo()
        time.sleep(2)

        # O primeiro no inicia o processo de escrever no vetor (token) vazio do anel e passar ao proximo no
        if self.id_no == 0:
            self.escrever_no_token() # Escreve no vetor
            self.enviar_para_proximo()

        while True:
            self.esperar_token() # Espera o vetor (token) do no anterior

            if self.verificar_regiao_critica():
                self.entrar_regiao_critica()
                self.sair_da_regiao_critica()

            else:
                self.escrever_no_token() # Escreve no vetor
                if self.queda == 2:
                    self.cair()
            
            self.enviar_para_proximo() # Envia o vetor para o proximo no

            self.rodadaPassandoToken += 1
            
if __name__ == "__main__":
    # import sys
    # if len(sys.argv) != 9:
    #     print("Uso: python3 no.py <id_no> <num_de_nos> <host> <porta_cliente> <porta_no_atual> <proximo_host> <proximo_port>")
    #     sys.exit(1)

    id_no = int(sys.argv[1])
    num_de_nos = int(sys.argv[2])
    host = sys.argv[3]
    porta_cliente = int(sys.argv[4])  # Porta para clientes
    porta_no_atual = int(sys.argv[5])  # Porta para comunicação entre nos
    proximo_host = sys.argv[6]
    proximo_port = int(sys.argv[7])
    proximo_proximo_host = sys.argv[8]
    queda = int(sys.argv[9]) if len(sys.argv) == 10 else -1

    no = No(id_no, num_de_nos, host, porta_cliente, porta_no_atual, proximo_host, proximo_port, proximo_proximo_host, queda)
    
    # Inicia a thread para escutar o cliente
    threading.Thread(target=no.escutar_cliente, daemon=True).start()
    
    # Executa o loop principal do no
    no.executar_no()
    