/*
Explora��o de dados da COVID-19

Habilidades usadas: Joins, CTE's, Tabelas Tempor�rias, Fun��es de Janela, Fun��es Agregadas, Cria��o de Visualiza��o, Convers�o de Tipos de Dados.

*/

SELECT *
FROM CovidDeaths
WHERE continent is not null
ORDER BY 3, 4

-- Selecionando dados iniciais que ser�o utilizados.

SELECT Location, date, total_cases, new_cases, total_deaths, population
FROM PortfolioProject..CovidDeaths
WHERE continent is not null
ORDER BY 1,2


-- Visualizando o Total de Casos vs Total de Mortes
-- � poss�vel visualizar quais eram as chances de morte, nas datas apresentadas, caso o indiv�duo fosse contaminado com o v�rus da COVID-19, no Brasil.
SELECT Location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS DeathPercentage
FROM PortfolioProject..CovidDeaths
WHERE continent is not null
AND location = 'Brazil'
ORDER BY 1,2


-- Visualizando o Total de Casos vs Popula��o
-- Pode-se visualizar a porcentagem da popula��o brasileira que foi contaminada com a COVID, nas datas que est�o inseridas no banco de dados.
SELECT Location, date, Population, total_cases, (total_cases/population)*100 AS PercentPopInfected
FROM PortfolioProject..CovidDeaths
WHERE continent is not null
AND location = 'Brazil'
ORDER BY 1,2


-- Visualizando os pa�ses com a maior taxa de contamina��o comparada com a popula��o
SELECT Location, Population, MAX(total_cases) AS HighestInfectionCount, MAX(total_cases/population)*100 AS PercentPopInfected
FROM PortfolioProject..CovidDeaths
WHERE continent is not null
GROUP BY Location, Population
ORDER BY PercentPopInfected DESC


-- Pa�ses com o maior n�mero de mortes comparada com a popula��o.
SELECT Location, MAX(cast(total_deaths as int)) AS TotalDeathCount
FROM PortfolioProject..CovidDeaths
WHERE continent is not null
GROUP BY Location
ORDER BY TotalDeathCount DESC


-- Por Continente
-- Continentes com as maiores quantidades de mortes comparadas com a popula��o, at� a data dispon�vel no banco de dados.
SELECT location, MAX(cast(total_deaths as int)) AS TotalDeathCountCont
FROM PortfolioProject..CovidDeaths
WHERE continent is null
GROUP BY location
ORDER BY TotalDeathCountCont DESC


-- N�mero de mortes na Am�rica do Sul.
SELECT location, MAX(cast(total_deaths as int)) AS TotalDeathCountCont
FROM PortfolioProject..CovidDeaths
WHERE location = 'South America' 
AND continent is null
GROUP BY location
ORDER BY TotalDeathCountCont DESC


-- N�mero de mortes na Am�rica do Norte.
SELECT location, MAX(cast(total_deaths as int)) AS TotalDeathCountCont
FROM PortfolioProject..CovidDeaths
WHERE location = 'North America' 
AND continent is null
GROUP BY location
ORDER BY TotalDeathCountCont DESC


-- N�mero de mortes na Europa.
SELECT location, MAX(cast(total_deaths as int)) AS TotalDeathCountCont
FROM PortfolioProject..CovidDeaths
WHERE location = 'Europe' 
AND continent is null
GROUP BY location
ORDER BY TotalDeathCountCont DESC


-- N�mero de mortes na �sia.
SELECT location, MAX(cast(total_deaths as int)) AS TotalDeathCountCont
FROM PortfolioProject..CovidDeaths
WHERE location = 'Asia' 
AND continent is null
GROUP BY location
ORDER BY TotalDeathCountCont DESC


-- N�mero de mortes na Oceania.
SELECT location, MAX(cast(total_deaths as int)) AS TotalDeathCountCont
FROM PortfolioProject..CovidDeaths
WHERE location = 'Oceania' 
AND continent is null
GROUP BY location
ORDER BY TotalDeathCountCont DESC




-- N�meros Globais
-- Acompanhamento de n�meros, por dia registrado, mundialmente.
SELECT date, SUM(new_cases) AS TotalCases, SUM(cast(new_deaths as int)) AS TotalDeaths, SUM(cast(new_deaths as int))/SUM(new_cases)*100 AS DeathPercentage
FROM PortfolioProject..CovidDeaths
WHERE continent is not null
GROUP BY date
ORDER BY 1,2


-- Total de casos, de mortes e sua porcentagem, visualizadas globalmente.
SELECT SUM(new_cases) AS TotalCases, SUM(cast(new_deaths as int)) AS TotalDeaths, SUM(cast(new_deaths as int))/SUM(new_cases)*100 AS DeathPercentage
FROM PortfolioProject..CovidDeaths
WHERE continent is not null
ORDER BY 1,2



-- Total da Popula��o vs Vacina��o
-- Porcentagem da popula��o que tenha recebido, pelo menos, uma doese da vacina.

SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int, vac.new_vaccinations)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS RollingPeopleVaccinated
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent is not null
ORDER BY 2, 3


-- Usando CTE para realizar c�lculos em Partition By na query anterior.

WITH PopvsVac (Continent, Location, date, Population, New_Vaccinations, RollingPeopleVaccinated)
AS
(
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int, vac.new_vaccinations)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS RollingPeopleVaccinated
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent is not null
-- ORDER BY 2, 3
)
SELECT *, (RollingPeopleVaccinated/Population)*100
FROM PopvsVac



-- Utilizando tableas tempor�rias para realizar c�lculo em Partition By na query anterior.

DROP TABLE IF EXISTS #PercentPopulationVaccinated
CREATE TABLE #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_vaccinations numeric,
RollingPeopleVaccinated numeric
)

INSERT INTO #PercentPopulationVaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int, vac.new_vaccinations)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS RollingPeopleVaccinated
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date

SELECT *, (RollingPeopleVaccinated/Population)*100
FROM #PercentPopulationVaccinated



-- Criando visualiza��o com o intuito de armazenar os dados para uma futura an�lise.

CREATE VIEW PercentPopulationVaccinated AS
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int, vac.new_vaccinations)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS RollingPeopleVaccinated
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent is not null


SELECT *
FROM PercentPopulationVaccinated