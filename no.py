import socket
import time
import random
import json
import threading

class No:
    def __init__(self, id_no, num_de_nos, host, port, proximo_host, proximo_port) -> None:
        self.id_no = id_no
        self.num_de_nos = num_de_nos
        self.host = host
        self.port = port
        self.proximo_host = proximo_host
        self.proximo_port = proximo_port
        self.vetor = [None] * num_de_nos  # Inicializa o vetor vazio
        self.timestamp_cliente = None
        self.token = None
        self.sleep_secao_critica = random.uniform(0.2, 1)  # Tempo na seção crítica
    
    def conectar_ao_proximo(self):
        """Conecta ao próximo nó no anel"""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.proximo_host, self.proximo_port))
        return s

    def escutar_cliente(self):
        """Escuta as requisições do cliente"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"Nó {self.id_no} esperando conexões de clientes...")

            while True:
                conn, addr = s.accept()
                with conn:
                    print(f"Nó {self.id_no} conectado com o cliente {addr}")
                    while True:
                        dados = conn.recv(1024)
                        if not dados:
                            break
                        mensagem = json.loads(dados.decode())
                        self.timestamp_cliente = mensagem.get('timestamp')
                        print(f"Nó {self.id_no} recebeu timestamp do cliente: {self.timestamp_cliente}")
                        # Envia a confirmação para o cliente
                        conn.sendall(json.dumps({"status": "commited"}).encode())

    def enviar_para_proximo(self, vetor):
        """Envia o vetor para o próximo nó no anel"""
        with self.conectar_ao_proximo() as s:
            s.sendall(json.dumps(vetor).encode())
    
    def esperar_token(self):
        """Espera o token (vetor) chegar de outro nó"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            conn, _ = s.accept()
            with conn:
                dados = conn.recv(1024)
                if dados:
                    self.token = json.loads(dados.decode())
                    print(f"Nó {self.id_no} recebeu token: {self.token}")

    def escrever_no_vetor(self):
        """Escreve no vetor o timestamp do cliente ou NULL"""
        if self.timestamp_cliente is not None:
            self.token[self.id_no] = self.timestamp_cliente
        else:
            self.token[self.id_no] = None
        print(f"Nó {self.id_no} escreveu no vetor: {self.token}")

    def verificar_secao_critica(self):
        """Verifica se deve entrar na seção crítica"""
        if all(item is None for item in self.token):
            return False

        menor_timestamp = min([t for t in self.token if t is not None])
        return self.timestamp_cliente == menor_timestamp
    
    def entrar_secao_critica(self):
        """Entra na seção crítica e simula o processamento"""
        print(f"Nó {self.id_no} entrando na seção crítica...")
        time.sleep(self.sleep_secao_critica)
        print(f"Nó {self.id_no} saiu da seção crítica.")
    
    def executar(self):
        """Executa o loop principal do nó"""
        if self.id_no == 0:
            # Nó 0 inicia o processo
            self.token = [None] * self.num_de_nos
            self.escrever_no_vetor()
            self.enviar_para_proximo(self.token)

        while True:
            self.esperar_token()  # Espera o vetor (token) do nó anterior
            self.escrever_no_vetor()  # Escreve no vetor
            if self.verificar_secao_critica():
                self.entrar_secao_critica()
                self.token[self.id_no] = None  # Limpa a posição do vetor após sair da seção crítica
            self.enviar_para_proximo(self.token)  # Envia o vetor para o próximo nó

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 6:
        print("Uso: python3 no.py <id_no> <num_de_nos> <host> <port> <proximo_host> <proximo_port>")
        sys.exit(1)

    id_no = int(sys.argv[1])
    num_de_nos = int(sys.argv[2])
    host = sys.argv[3]
    port = int(sys.argv[4])
    proximo_host = sys.argv[5]
    proximo_port = int(sys.argv[6])

    no = No(id_no, num_de_nos, host, port, proximo_host, proximo_port)
    
    # Inicia a thread para escutar o cliente
    threading.Thread(target=no.escutar_cliente, daemon=True).start()
    
    # Executa o loop principal do nó
    no.executar()
