import os
import matplotlib.pyplot as plt


def get_start_dates_tlg(root='Telegram data'):
    files = os.listdir(f'{root}')
    files.remove('.DS_Store')

    fig, axs = plt.subplots(nrows=len(files)//3 + 1, ncols=3, figsize=(25, 50))
    plt.subplots_adjust(hspace=0.5)
    fig.suptitle("Количество новостей по каналам по дням", fontsize=30)
    fig.tight_layout()

    for file, ax in zip(files, axs.ravel()):
        tmp = pd.read_json(f'{root}/{file}')['date'].dt.round('D')
        date = tmp.iloc[-1]
        ax.hist(tmp, bins=len(np.unique(tmp)))
        ax.set_title(f'{file[:-14]} начал работу {date}')
    plt.show()
    
def get_dates_tlg(root='Telegram data'):
    files = os.listdir(f'{root}')
    files.remove('.DS_Store')
    dates = pd.Series()
    for file in files:
        tmp = pd.to_datetime(pd.read_json(f'{root}/{file}')['date']).dt.round('D')
        dates = pd.concat([dates, tmp], axis=0)
    
    return dates

def dates_news(roots):
    fig, axs = plt.subplots(nrows=len(roots)//3 + 1, ncols=3, figsize=(25, 50))
    plt.subplots_adjust(hspace=0.5)
    fig.suptitle("Количество новостей по категориям изданий", fontsize=30)
    fig.tight_layout()

    for root, ax in zip(roots, axs.ravel()):
        date = pd.read_csv(f'{root}/{root}.csv')['date']
        date = pd.to_datetime(date).dt.round('D')
        
        ax.hist(date, bins=len(np.unique(date)))
        ax.set_title(f'{root}')

    plt.show()
    
def avg_time_news(roots):
    df = pd.DataFrame()
    for root in roots:
        date = pd.read_csv(f'{root}/{root}.csv')['date']
        date = pd.to_datetime(date)
        delta = (date - date.shift(1)).dt.total_seconds() 
        avg_time = delta.mean() / 60
        max_time = delta.max() / 60
        min_time = delta.min() / 60
        times = pd.DataFrame([[root, min_time, avg_time, max_time]], columns=['Источник', 'Минимальное время', 'Среднее время', 'Максимальное время'])
        df = pd.concat([df, times], axis=0).reset_index(drop=True)
    return df


def avg_time_tg(root):
    roots = os.listdir(f'{root}/Telegram prep')
    roots.remove('.DS_Store')
    roots.remove('messages')
    df = pd.DataFrame()
    for root in roots:
        date = pd.read_csv(f'Telegram prep/{root}', low_memory=False)['date']
        date = pd.to_datetime(date)
        delta = (date - date.shift(1)).dt.total_seconds()
        avg_time = delta.mean() / 60
        max_time = delta.max() / 60
        min_time = delta.min() / 60
        times = pd.DataFrame([[root[:-4], min_time, avg_time, max_time]], columns=['Источник', 'Минимальное время', 'Среднее время', 'Максимальное время'])
        df = pd.concat([df, times], axis=0).reset_index(drop=True)
    return df


def plot_company_news_hist(roots, companies):
    cols = ['date'] + [i for i in companies.keys()]
    agg_df = pd.DataFrame()
    for root in roots:
        df = pd.read_csv(f'{root}/{root}.csv')
        df = df[cols]
        agg_df = pd.concat([agg_df, df])
    
    agg_df['date'] = pd.to_datetime(agg_df['date'])
    
    fig, axs = plt.subplots(nrows=len(companies)//3 + 1, ncols=3, figsize=(25, 50))
    plt.subplots_adjust(hspace=0.5)
    fig.suptitle("Количество новостей по компаниям", fontsize=30, y=1.02)
    fig.tight_layout()

    for company, ax in zip(companies, axs.ravel()):
        date = agg_df[agg_df[company] == True]['date'].dt.round('D')
        
        try:
            ax.hist(date, bins=len(np.unique(date)))
        except ValueError as ex:
            print(ex)
            print(f'{company}')

        ax.set_title(f'{company}; {len(date)} news')

    plt.show()


def plot_company_news_hist_tg(companies):
    cols = ['date'] + [i for i in companies.keys()]
    agg_df = pd.DataFrame()
    roots = os.listdir('Telegram prep')
    if '.DS_Store' in roots:
        roots.remove('.DS_Store')
    if 'messages' in roots:
        roots.remove('messages')
    for root in roots:
        df = pd.read_csv(f'Telegram prep/{root}', low_memory=False)
        df = df[cols]
        agg_df = pd.concat([agg_df, df])
    
    agg_df['date'] = pd.to_datetime(agg_df['date'])
    
    fig, axs = plt.subplots(nrows=len(companies)//3 + 1, ncols=3, figsize=(25, 50))
    plt.subplots_adjust(hspace=0.5)
    fig.suptitle("Количество новостей по компаниям", fontsize=30, y=1.02)
    fig.tight_layout()

    for company, ax in zip(companies, axs.ravel()):
        date = agg_df[agg_df[company] == True]['date'].dt.round('D')
        
        try:
            ax.hist(date, bins=len(np.unique(date)))
        except ValueError as ex:
            print(ex)
            print(f'{company}')
            
        ax.set_title(f'{company}; {len(date)} news')

    plt.show()

    
def plot_sector_news_hist(roots, sectors):
    cols = ['date'] + [i for i in sectors]
    agg_df = pd.DataFrame()
    for root in roots:
        df = pd.read_csv(f'{root}/{root}.csv')
        df = df[cols]
        agg_df = pd.concat([agg_df, df])
    
    agg_df['date'] = pd.to_datetime(agg_df['date'])
    
    fig, axs = plt.subplots(nrows=len(sectors)//3 + 1, ncols=3, figsize=(25, 50))
    plt.subplots_adjust(hspace=0.5)
    fig.suptitle("Количество новостей по секторам", fontsize=30, y=1.02)
    fig.tight_layout()

    for sector, ax in zip(sectors, axs.ravel()):
        date = agg_df[agg_df[sector] == True]['date'].dt.round('D')
        
        try:
            ax.hist(date, bins=len(np.unique(date)))
        except ValueError as ex:
            print(ex)
            print(f'{sector}')

        ax.set_title(f'{sector}; {len(date)} news')

    plt.show()

    
def plot_sector_news_hist_tg(sectors):
    cols = ['date'] + [i for i in sectors]
    agg_df = pd.DataFrame()
    roots = os.listdir('Telegram prep')
    if '.DS_Store' in roots:
        roots.remove('.DS_Store')
    if 'messages' in roots:
        roots.remove('messages')
    for root in roots:
        df = pd.read_csv(f'Telegram prep/{root}', low_memory=False)
        df = df[cols]
        agg_df = pd.concat([agg_df, df])
    
    agg_df['date'] = pd.to_datetime(agg_df['date'])
    
    fig, axs = plt.subplots(nrows=len(sectors)//3 + 1, ncols=3, figsize=(25, 50))
    plt.subplots_adjust(hspace=0.5)
    fig.suptitle("Количество новостей по секторам", fontsize=30, y=1.02)
    fig.tight_layout()

    for sector, ax in zip(sectors, axs.ravel()):
        date = agg_df[agg_df[sector] == True]['date'].dt.round('D')
        
        try:
            ax.hist(date, bins=len(np.unique(date)))
        except ValueError as ex:
            print(ex)
            print(f'{sector}')
            
        ax.set_title(f'{sector}; {len(date)} news')

    plt.show()