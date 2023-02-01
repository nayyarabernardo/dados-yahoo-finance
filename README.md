# Airflow Yfinance API Project

Este projeto é uma implementação de fluxo de trabalho de análise de dados financeiros utilizando a plataforma Apache Airflow e a API Yfinance. A API Yfinance fornece informações financeiras em tempo real e históricas para uma ampla gama de ativos, incluindo ações, fundos imobiliários e criptomoedas.

Neste projeto específico, utilizamos a API Yfinance para baixar dados financeiros dos quatro maiores varejistas do Brasil, listados abaixo:

    CRFB3.SA - Carrefour
    ASAI3.SA - GPA
    AMER3.SA - Lojas Americanas
    MGLU3.SA - Magazine Luiza

Com o objetivo de realizar análises financeiras sobre essas empresas e, assim, ajudar investidores a tomar decisões informadas de investimento.

As operações DAG incluídas neste projeto baixam dados financeiros dessas empresas, realizam análises e armazenam os resultados em um banco de dados relacional. As informações dos ativos que desejam ser analisados, incluindo símbolo de ativo, período de tempo e frequência de atualização, podem ser configuradas nas variáveis de ambiente no arquivo config.py.

Este projeto é uma ferramenta valiosa para investidores interessados em obter insights financeiros sobre as maiores empresas de varejo do Brasil, especialmente sobre a Lojas Americanas.

Este projeto é uma implementação de fluxo de trabalho de análise de dados financeiros utilizando a plataforma Apache Airflow (https://airflow.apache.org/)e a API Yfinance (https://pypi.org/project/yfinance/).
Instalação

Para utilizar este projeto, você precisará ter o Apache Airflow instalado em sua máquina. As instruções de instalação podem ser encontradas em airflow.apache.org/docs/stable/installation.html.

Além disso, você precisará instalar o módulo yfinance através do comando pip install yfinance.

##Uso

Este projeto inclui operações DAG que baixam dados financeiros de ativos específicos da API Yfinance, realizam análises e armazenam os resultados em um banco de dados relacional.

Você pode configurar as informações dos ativos que deseja analisar, incluindo símbolo de ativo, período de tempo e frequência de atualização, nas variáveis de ambiente no arquivo config.py.

##Contribuição

Este projeto é aberto a contribuições. Se você deseja melhorar ou adicionar recursos, sinta-se à vontade para criar uma solicitação pull ou entrar em contato.

##Licença

Este projeto está disponível sob a licença MIT.
