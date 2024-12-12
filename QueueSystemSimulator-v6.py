# Diego Malta
# Felipe Melo

import heapq
import math
import random
import numpy as np
import argparse

class Evento:
    def __init__(self, time, tipo_evento, job_id:int=None, servidor:int=None, tempo_de_chegada=None):
        self.time = time
        self.tipo_evento = tipo_evento
        self.job_id = job_id
        self.servidor = servidor
        self.tempo_de_chegada = tempo_de_chegada

    def __lt__(self, other):
        return self.time < other.time

class Servidor:
    def __init__(self, distribuicao_tempo_de_servico):
        self.distribuicao_tempo_de_servico = distribuicao_tempo_de_servico
        self.queue = []
        self.ocupado = False

class RedeDeFilas:
    def __init__(self, taxa_de_chegada:int, distribuicao_tempo_de_servico, warmup_jobs, jobs_validos, seed=42): # seed=42 porque 42 é a resposta para a vida, o universo e tudo mais
        self.seed = seed
        random.seed(seed)
        np.random.seed(seed)

        self.taxa_de_chegada = taxa_de_chegada
        self.distribuicao_tempo_de_servico = distribuicao_tempo_de_servico

        self.servidores = [
            Servidor(distribuicao_tempo_de_servico[0]),
            Servidor(distribuicao_tempo_de_servico[1]),
            Servidor(distribuicao_tempo_de_servico[2])
        ]

        self.eventos = []
        self.tempo_atual = 0
        self.contador_jobs = 0

        self.tempos_no_sistema = []
        self.warmup_jobs = warmup_jobs
        self.jobs_validos = jobs_validos

    def exponencial_com_inversa_da_CDF(self, rate):
        return -math.log(1 - random.random()) / rate

    def calcular_tempo_de_servico(self, servidor_index):
        funcao_distribuicao = self.distribuicao_tempo_de_servico[servidor_index]
        return funcao_distribuicao()

    def calcula_proxima_chegada(self):
        time = self.tempo_atual + self.exponencial_com_inversa_da_CDF(self.taxa_de_chegada)
        evento_de_chegada = Evento(time, 'chegada')
        heapq.heappush(self.eventos, evento_de_chegada)

    def processa_chegada(self, evento):
        job_id = self.contador_jobs
        self.contador_jobs += 1
        tempo_de_chegada = evento.time

        servidor = self.servidores[0]
        tempo_de_servico = self.calcular_tempo_de_servico(0)

        if not servidor.ocupado:
            servidor.ocupado = True
            tempo_de_partida = evento.time + tempo_de_servico
            evento_de_saida = Evento(
                tempo_de_partida,
                'saida',
                job_id,
                servidor=0,
                tempo_de_chegada=tempo_de_chegada
            )
            heapq.heappush(self.eventos, evento_de_saida)
        else:
            servidor.queue.append((job_id, tempo_de_chegada))

        self.calcula_proxima_chegada()

    def processa_saida(self, evento):
        index_servidor_atual = evento.servidor
        job_id = evento.job_id
        servidor = self.servidores[index_servidor_atual]

        servidor.ocupado = False

        if index_servidor_atual == 0:
            if random.random() < 0.5:
                index_proximo_servidor = 1  #servidor 2
            else:
                index_proximo_servidor = 2  #servidor 3
        elif index_servidor_atual == 1:
            if random.random() < 0.2:
                index_proximo_servidor = 1
            else:
                index_proximo_servidor = -1  #sai do sistema
        else:
            index_proximo_servidor = -1  #sai do sistema

        if index_proximo_servidor != -1:
            proximo_servidor = self.servidores[index_proximo_servidor]
            tempo_de_servico = self.calcular_tempo_de_servico(index_proximo_servidor)

            if not proximo_servidor.ocupado:
                proximo_servidor.ocupado = True
                tempo_de_partida = self.tempo_atual + tempo_de_servico
                evento_de_saida = Evento(
                    tempo_de_partida,
                    'saida',
                    job_id,
                    servidor = index_proximo_servidor,
                    tempo_de_chegada = evento.tempo_de_chegada
                )
                heapq.heappush(self.eventos, evento_de_saida)
            else:
                proximo_servidor.queue.append((job_id, self.tempo_atual))
        else:
            # registrando o tempo de quando um job sai do sistema no array
            if job_id >= self.warmup_jobs:
                self.tempos_no_sistema.append(self.tempo_atual - evento.tempo_de_chegada)

        # processa o próximo job na fila do servidor atual
        if servidor.queue:
            proximo_job_id, tempo_de_chegada = servidor.queue.pop(0)
            tempo_de_servico = self.calcular_tempo_de_servico(index_servidor_atual)
            servidor.ocupado = True
            tempo_de_partida = self.tempo_atual + tempo_de_servico
            evento_de_saida = Evento(
                tempo_de_partida,
                'saida',
                proximo_job_id,
                servidor = index_servidor_atual,
                tempo_de_chegada = tempo_de_chegada
            )
            heapq.heappush(self.eventos, evento_de_saida)

    def executar_simulacao(self):
        self.calcula_proxima_chegada()

        while len(self.tempos_no_sistema) < self.jobs_validos:
            if not self.eventos:
                break

            evento = heapq.heappop(self.eventos)
            self.tempo_atual = evento.time

            if evento.tipo_evento == 'chegada':
                self.processa_chegada(evento)
            elif evento.tipo_evento == 'saida':
                self.processa_saida(evento)

        mean_time = np.mean(self.tempos_no_sistema)
        std_time = np.std(self.tempos_no_sistema)
        return mean_time, std_time

def distribuicao_deterministica():
    return lambda: 0.4, lambda: 0.6, lambda: 0.95

def distribuicao_uniforme():
    return (
        lambda: random.uniform(0.1, 0.7),
        lambda: random.uniform(0.1, 1.1),
        lambda: random.uniform(0.1, 1.8)
    )

def distribuicao_exponencial():
    return (
        lambda: random.expovariate(1/0.4),
        lambda: random.expovariate(1/0.6),
        lambda: random.expovariate(1/0.95)
    )

def main():
    parser = argparse.ArgumentParser(description='Simulação de uma rede de filas')
    parser.add_argument('warmup_jobs', type=int, default=1000000, help='Número de jobs para o período de aquecimento')
    parser.add_argument('jobs_validos', type=int, default=10000, help='Número de jobs válidos para o cálculo das métricas')
    args = parser.parse_args()
    sistema = RedeDeFilas(taxa_de_chegada=2, distribuicao_tempo_de_servico=distribuicao_deterministica(),warmup_jobs=args.warmup_jobs, jobs_validos=args.jobs_validos)
    mean_time, std_time = sistema.executar_simulacao()
    print(f"Situação 1: Tempo médio no sistema = {mean_time:.4f}s, Desvio padrão = {std_time:.4f}s")
    sistema = RedeDeFilas(taxa_de_chegada=2, distribuicao_tempo_de_servico=distribuicao_uniforme(), warmup_jobs=args.warmup_jobs, jobs_validos=args.jobs_validos)
    mean_time, std_time = sistema.executar_simulacao()
    print(f"Situação 2: Tempo médio no sistema = {mean_time:.4f}s, Desvio padrão = {std_time:.4f}s")
    sistema = RedeDeFilas(taxa_de_chegada=2, distribuicao_tempo_de_servico=distribuicao_exponencial(), warmup_jobs=args.warmup_jobs, jobs_validos=args.jobs_validos)
    mean_time, std_time = sistema.executar_simulacao()
    print(f"Situação 3: Tempo médio no sistema = {mean_time:.4f}s, Desvio padrão = {std_time:.4f}s")

if __name__ == '__main__':
    main()