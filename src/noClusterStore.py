# Logica: (lembrando que o no replica funciona como um backup do no primario)

# 1- os nos replicas do cluster store estabele conexao com no primario (de escuta e de envio) e o no primario estabelece essas respectivas conexoes

# 2- fica escutando para receber requisicao de algum elemento do cluster sync
    # 2.1 - aceitar conexao
    # 2.2 - receber a requisicao

# 3- caso a requisicao recebida seja de escrita:
    # 3.1- se o no que recebeu for o primario: 
        # 3.1.1- executa a escrita
        # 3.1.2- manda a atualização para os nos de backup
        # 3.1.3- recebe o retorno de reconhecimento da atualizacao dos nos de backup
        # 3.1.4- retorna ao elemento do cluster sync um reconhecimento que a escrita foi concluida

    # 3.2- se o no que recebeu for o de backup:
        # 3.2.1- envia a requisicao de escrita para o no primario
        # 3.2.2- recebe do no primario atualizacao a ser feita
        # 3.2.3- retorna ao no primario um reconhecimento da atualizacao
        # 3.2.4- retorna ao elemento do cluster sync um reconhecimento que a escrita foi concluida

# 4- finaliza conexao com aquele elemento do cluster sync e fica escutando para receber alguma requisicao de outro elemento, retornando ao #2

#! por fim, fazer alteracoes no codigo do cluster sync para acessar o cluster store ao entrar na regiao critica

import threading
import socket
from constants import * 
import json

class  noClusterStore:
    def __init__(self, id, host, portaRequisicao, porta1 = None, porta2 = None, porta_no_primario = None, host_no_primario = None):
        self.id = id
        self.host = host
        self.portaRequisicao = portaRequisicao # porta para receber requisicoes do cluster sync
        self.primario = True if id == 0 else False # representa se o no eh um no primario do cluster store ou um no de backup
        self.armazenamento = ""

        if self.pimario:
            self.porta1 = porta1 # porta com qual um dos nos de backup do cluster store fara conexao
            self.porta2 = porta2 # porta com qual um dos nos de backup do cluster store fara conexao
            self.conn_backup1 = None
            self.conn_backup2 = None

            self.no_backup1_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.no_backup2_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        else:
            self.porta_no_primario = porta_no_primario # porta para estabelecer conexao com o no primario
            self.host_no_primario = host_no_primario # host do no primario para estabelecer conexao com ele
            
            self.no_primario_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.no_clusterSync_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.mutex = threading.Lock()
            

    def estabeleConexoesDoCluster(self):
        if self.primario:
            conexaoPrimarioBackup1 = threading.Thread(target=self.noPrimarioConexaoBackup1, daemon=True)
            conexaoPrimarioBackup2 = threading.Thread(target=self.noPrimarioConexaoBackup2, daemon=True)

            conexaoPrimarioBackup1.start()
            conexaoPrimarioBackup2.start()

            conexaoPrimarioBackup1.join()
            conexaoPrimarioBackup2.join()
            
        else:
            self.no_primario_socket.connect((self.host_no_primario, self.porta_no_primario))

    # no primario estabelecendo conexao com o primeiro no de backup
    def noPrimarioConexaoBackup1(self):
        self.no_backup1_socket.bind((self.host, self.porta))
        self.no_backup1_socket.listen()
        self.conn_backup1, _ = self.no_backup1_socket.accept()

    # no primario estabelecendo conexao com o segundo no de backup
    def noPrimarioConexaoBackup2(self):
        self.no_backup2_socket.bind((self.host, self.porta2))
        self.no_backup2_socket.listen()
        self.conn_backup2, _ = self.no_backup2_socket.accept()

    # executando em uma thread separada
    def escutaClusterSync(self):
        while True:
            # Fazendo o binding
            self.no_clusterSync_socket.bind((self.host, self.portaRequisicao))
            self.no_clusterSync_socket.listen()

            conn, addr = self.no_clusterSync_socket.accept()
        
            with conn:
                with self.mutex:
                    self.clienteConectado = True
                    print(f"Nó {self.id} conectado com o elemento do cluster sync {addr}")

                
                dados = conn.recv(BUFFER_SIZE)
                mensagem = json.loads(dados.decode())
                
                if self.primario:
                    self.noPrimarioExecutandoRequisicao(mensagem)

                else:
                    # envia a requisicao de escrita para o no primario
                    self.no_primario_socket.sendall(json.dumps(mensagem).encode())

                    # recebe do no primario atualizacao a ser feita
                    atualizacao = self.no_primario_socket.recv(BUFFER_SIZE).decode()
                    atualizacao = json.loads(atualizacao)
                    
                    self.noBackupExecutandoAtualizacao(atualizacao)

                # retornando ao elemento do cluster sync um reconhecimento que a escrita foi concluida
                conn.sendall(json.dumps({"status": "escrita concluída"}).encode())

    # fica escutando os nos backup caso eu seja um no primario, para receber requisicao repassada por eles
    # fica escutando o no primario caso eu seja um no backup, para receber uma atualizacao vinda dele
    def escutaClusterStore(self):
        if self.primario:
            threading.Thread(target=self.noEsperandoRequisicaoAtualizacao, args=(self.conn_backup1,), daemon=True).start()
            threading.Thread(target=self.noEsperandoRequisicaoAtualizacao, args=(self.conn_backup2,), daemon=True).start()

        else:
            self.noEsperandoRequisicaoAtualizacao(self.no_primario_socket)

    # no primario recebendo requisicao repassada de um no de backup do cluster store, ou no backup recebendo atualizacao vinda do no primario
    def noEsperandoRequisicaoAtualizacao(self, connection):
        while True:
            dados = connection.recv(BUFFER_SIZE)
            mensagem = json.loads(dados.decode())

            if self.primario: 
                self.noPrimarioExecutandoRequisicao(mensagem)
            else:
                self.noBackupExecutandoAtualizacao(mensagem)

    def noBackupExecutandoAtualizacao(self, atualizacao):
        # recebe do no primario atualizacao a ser feita
        self.armazenamento = atualizacao

        # retorna ao no primario um reconhecimento da atualizacao
        self.no_primario_socket.sendall(json.dumps("atualização concluída").encode())

    # no primario executando a requisicao e enviando a atualizacao aos nos de backup
    def noPrimarioExecutandoRequisicao(self, mensagem):
        # executando a requisicao de escrita
        self.armazenamento = mensagem

        # manda a atualização para os nos de backup
        # recebe o retorno de reconhecimento da atualizacao dos nos de backup
        self.conn_backup1.sendall(json.dumps(self.armazenamento).encode())
        retorno1 = self.conn_backup1.recv(BUFFER_SIZE).decode()
        retorno1 = json.loads(retorno1)

        self.conn_backup2.sendall(json.dumps(self.armazenamento).encode())
        retorno2 = self.conn_backup2.recv(BUFFER_SIZE).decode()
        retorno2 = json.loads(retorno2)
        
        if(retorno1 == "atualização concluída" and retorno2 == "atualização concluída"):
            print("ok")
        else:
            print("Erro na atualização de um nó backup!")

    def executar_no(self):
        self.estabeleConexoesDoCluster()

        threading.Thread(target=self.escutaClusterSync, daemon=True).start()

        self.escutaClusterStore()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 5 and len(sys.argv) != 7:
        print("Uso: python3 noClusterStore.py <id_no> <host> <porta_para_requisicao_do_cluster_sync> <porta_para_conexao_backup1> <porta_para_conexao_backup2>"
              +"\nou\n"+
              "Uso: python3 noClusterStore.py <id_no> <host> <porta_para_requisicao_do_cluster_sync> 0 0 <porta_do_no_primario> <host_do_no_primario>")
        sys.exit(1)

    id_no = int(sys.argv[1])
    host = sys.argv[2]
    porta_requisicao = int(sys.argv[3])  # porta para requisicao do cluster sync

    if id_no == 0:
        porta1 = int(sys.argv[4]) # porta para conexao do primeiro no de backup
        porta2 = int(sys.argv[5]) # porta para conexao do segundo no de backup

        no = noClusterStore(id_no, host, porta_requisicao, porta1, porta2)
    
    else:
        porta_no_primario = int(sys.argv[6]) # porta do no primario
        host_no_primario = sys.argv[7] # host do no primario

        no = noClusterStore(id_no, host, porta_requisicao, None, None, porta_no_primario, host_no_primario)
    
    # Executa o loop principal do no
    no.executar_no()