# IMDB Web Scraper with Async/Threading Hybrid Approach

Um web scraper moderno e eficiente para o IMDB, implementado em Python, demonstrando técnicas avançadas de programação concorrente. Este projeto combina programação assíncrona (asyncio) e multithreading para otimizar a coleta de dados de filmes.

## Características Principais

- Implementação híbrida: escolha entre asyncio ou multithreading
- Sistema robusto de rate limiting e retry
- Logging detalhado para monitoramento
- Salvamento automático em CSV com timestamp
- Tratamento de erros abrangente
- Documentação completa do código

## Tecnologias Utilizadas

- Python 3.x
- aiohttp para requisições assíncronas
- BeautifulSoup4 para parsing HTML
- concurrent.futures para threading
- Logging para monitoramento de execução

## Funcionalidades

- Extração de título, data de lançamento, avaliação e sinopse dos filmes
- Performance otimizada para grandes volumes de dados
- Flexibilidade para escolher entre processamento assíncrono ou threading
- Sistema de retry para lidar com falhas de conexão

## Uso Educacional

Este projeto serve como exemplo prático de:
- Programação assíncrona vs Threading
- Web Scraping responsável
- Boas práticas de código Python
- Tratamento de concorrência em aplicações web

---
⚠️ Nota: Como o IMDB atualiza sua estrutura periodicamente, pode ser necessário ajustar os seletores HTML. Mantenha o código atualizado para garantir seu funcionamento correto.