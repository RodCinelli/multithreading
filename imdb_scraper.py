import aiohttp
import asyncio
import csv
import random
from bs4 import BeautifulSoup
from typing import Optional, Dict, List
from datetime import datetime
import logging
import concurrent.futures
from functools import partial

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IMDBScraper:
    def __init__(self, max_threads=10):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                        '(KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
        }
        self.base_url = 'https://www.imdb.com'
        self.output_file = f'movies_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        self.session: Optional[aiohttp.ClientSession] = None
        self.max_threads = max_threads
        self.thread_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_threads,
            thread_name_prefix="IMDBScraper"
        )

    async def initialize(self):
        """Inicializa a sessão HTTP assíncrona"""
        self.session = aiohttp.ClientSession(headers=self.headers)

    async def close(self):
        """Fecha a sessão HTTP"""
        if self.session:
            await self.session.close()

    async def fetch_page(self, url: str) -> str:
        """Busca uma página com retry em caso de falha"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    logger.warning(f"Status code {response.status} for {url}")
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(random.uniform(1, 3))
        return ""

    async def extract_movie_details(self, movie_url: str) -> Optional[Dict]:
        """Extrai detalhes de um filme específico"""
        await asyncio.sleep(random.uniform(0.5, 1))  # Rate limiting
        html = await self.fetch_page(movie_url)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        try:
            # Encontrando a seção específica
            page_section = soup.find('section', class_='ipc-page-section')
            if not page_section:
                return None

            # Extraindo informações
            title_tag = soup.find('h1')
            title = title_tag.find('span').get_text() if title_tag else None

            date_tag = soup.find('a', href=lambda h: h and 'releaseinfo' in h)
            date = date_tag.get_text().strip() if date_tag else None

            rating_tag = soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
            rating = rating_tag.get_text() if rating_tag else None

            plot_tag = soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
            plot = plot_tag.get_text().strip() if plot_tag else None

            if all([title, date, rating, plot]):
                return {
                    'title': title,
                    'date': date,
                    'rating': rating,
                    'plot': plot
                }
        except Exception as e:
            logger.error(f"Error extracting details from {movie_url}: {str(e)}")
        
        return None

    async def save_to_csv(self, movies: List[Dict]):
        """Salva os filmes em um arquivo CSV"""
        if not movies:
            logger.warning("No movies to save")
            return

        fieldnames = ['title', 'date', 'rating', 'plot']
        try:
            with open(self.output_file, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(movies)
            logger.info(f"Successfully saved {len(movies)} movies to {self.output_file}")
        except Exception as e:
            logger.error(f"Error saving to CSV: {str(e)}")

    async def extract_movie_links(self, main_page_url: str) -> List[str]:
        """Extrai links dos filmes da página principal"""
        html = await self.fetch_page(main_page_url)
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'})
        if not movies_table:
            return []

        movies_list = movies_table.find('ul')
        if not movies_list:
            return []

        return [
            f"{self.base_url}{movie.find('a')['href']}"
            for movie in movies_list.find_all('li')
            if movie.find('a') and movie.find('a').get('href')
        ]

    def process_chunk_with_threads(self, movie_links: List[str]) -> List[Dict]:
        """Processa um grupo de links usando threads"""
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            movies = list(executor.map(self.extract_movie_details_sync, movie_links))
        return [m for m in movies if m]

    def extract_movie_details_sync(self, movie_url: str) -> Optional[Dict]:
        """Versão síncrona do extract_movie_details para uso com threads"""
        import requests
        try:
            response = requests.get(movie_url, headers=self.headers)
            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Encontrando a seção específica
            page_section = soup.find('section', class_='ipc-page-section')
            if not page_section:
                return None

            # Extraindo informações
            title_tag = soup.find('h1')
            title = title_tag.find('span').get_text() if title_tag else None

            date_tag = soup.find('a', href=lambda h: h and 'releaseinfo' in h)
            date = date_tag.get_text().strip() if date_tag else None

            rating_tag = soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
            rating = rating_tag.get_text() if rating_tag else None

            plot_tag = soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
            plot = plot_tag.get_text().strip() if plot_tag else None

            if all([title, date, rating, plot]):
                return {
                    'title': title,
                    'date': date,
                    'rating': rating,
                    'plot': plot
                }

        except Exception as e:
            logger.error(f"Error in thread processing {movie_url}: {str(e)}")
        return None

    async def run(self, use_threads=False):
        """Executa o scraping principal com opção de usar threads"""
        try:
            await self.initialize()
            start_time = datetime.now()
            logger.info(f"Starting IMDB scraping using {'threads' if use_threads else 'async'}")

            # Obtém links dos filmes
            movie_links = await self.extract_movie_links(f"{self.base_url}/chart/moviemeter/?ref_=nv_mv_mpm")
            if not movie_links:
                logger.error("No movie links found")
                return

            # Processa os filmes usando threads ou async
            if use_threads:
                # Usando threads
                movies = self.process_chunk_with_threads(movie_links)
            else:
                # Usando async
                tasks = [self.extract_movie_details(link) for link in movie_links]
                movies = await asyncio.gather(*tasks)
                movies = [m for m in movies if m]

            # Salva os resultados
            await self.save_to_csv(movies)

            duration = datetime.now() - start_time
            logger.info(f"Scraping completed in {duration.total_seconds():.2f} seconds")
            logger.info(f"Total movies scraped: {len(movies)}")

        except Exception as e:
            logger.error(f"Error in main execution: {str(e)}")
        finally:
            await self.close()

async def main():
    # Exemplo de uso com ambos os métodos
    scraper = IMDBScraper(max_threads=10)
    
    # Executando com async
    logger.info("Running with async...")
    await scraper.run(use_threads=False)
    
    # Executando com threads
    logger.info("Running with threads...")
    await scraper.run(use_threads=True)

if __name__ == '__main__':
    asyncio.run(main())