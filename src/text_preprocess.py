def lemm_file(root, file_path):
    try:
        df = pd.DataFrame(pd.read_pickle(f'{root}/raw_cites/{file_path}'))
        # df = pd.DataFrame(pd.read_pickle(f'ria_economy/splitted file/{file_path}'))
        df['tp'] = df['data_or_ex'].map(lambda x: isinstance(x, tuple))
        df.drop(df[df.tp != True].index, inplace=True)
        df[['date', 'title', 'announce', 'text']] = pd.DataFrame(df['data_or_ex'].tolist(), index=df.index)
        df.drop(columns=['data_or_ex', 'tp'], inplace=True)
        # df.drop(columns=['data_or_ex'], inplace=True)
        cols = ['title', 'announce', 'text']
        for col in cols:
            df[col] = df[col].str.lower()
            df[col] = df[col].str.replace('\n', '')
            df[col] = df[col].str.replace('\W+', ' ', regex=True)
            res = []
            doc = []
            large_str = ' '.join([txt + ' splitter ' for txt in df[col]])
            large_str = mystem.lemmatize(large_str)

            for word in large_str:
                if word.strip() != '' and word not in swords:
                    if word == 'splitter':
                        res.append(doc)
                        doc = []
                    else:
                        doc.append(word)
            del large_str
            del doc
            res = [' '.join(lst) for lst in res]
            df[col] = res
            print(f'{col} ready')
        return df
    except KeyboardInterrupt:
        raise
    except Exception as ex:
        print(ex)
        return None
    
def preprocess_tg(file):
    try:
        df = pd.read_json(f'Telegram data/{file}')[['date', 'message', 'views', 'forwards', 'fwd_from']].dropna(subset='message')
        n_rows = df.shape[0]
        c = 0
        cols = ['message']
        for col in cols:
            df[col] = df[col].str.replace('\n', '')
            df[col] = df[col].str.replace('\W+', ' ', regex=True)
            df[col] = df[col].str.lower()
            while c < n_rows:
                res = []
                doc = []
                large_str = ' '.join([txt + ' splitter ' for txt in df[col][c:c+1000]])
                large_str = mystem.lemmatize(large_str)

                for word in large_str:
                    if word.strip() != '' and word not in swords:
                        if word == 'splitter':
                            if len(doc) == 0:
                                doc.append('')
                            res.append(doc)
                            doc = []
                        else:
                            doc.append(word)
                del large_str
                del doc
                res = [' '.join(lst) for lst in res]
                df[col][c:c+1000] = res
                c += 1000
            print(f'{col} ready')
        return df
    except KeyboardInterrupt:
        raise
    except Exception as ex:
        print(ex)
        return None
    
    
def add_company(roots, for_regex, companies):
    for root in roots:
        df = pd.read_csv(f'{root}/{root}.csv')
        for company in companies:
            reg = for_regex[company]
            reg = ' | '.join(reg)
            in_str = df['title'].str.contains(reg) | df['announce'].str.contains(reg) | df['text'].str.contains(reg)
            df[company] = in_str
        df.to_csv(f'{root}/{root}.csv', index=False)
        print(f'{root} done')
        

def add_company_tg(for_regex, companies):
    files = os.listdir('Telegram prep')
    files.remove('messages')
    if '.DS_Store' in files:
        files.remove('.DS_Store')
    for file in files:
        df = pd.read_csv(f'Telegram prep/{file}', low_memory=False)
        for company in companies:
            reg = for_regex[company]
            reg = ' | '.join(reg)
            in_str = df['message'].str.contains(reg)
            df[company] = in_str
        df.to_csv(f'Telegram prep/{file}', index=False)
        print(f'{file} done')
        
        
def add_industry(roots, industries, for_regex_industry):
    for root in roots:
        df = pd.read_csv(f'{root}/{root}.csv')
        for industry in industries:
            reg = for_regex_industry[industry]
            reg = ' | '.join(reg)
            in_str = df['title'].str.contains(reg) | df['announce'].str.contains(reg) | df['text'].str.contains(reg)
            df[industry] = in_str
        df.to_csv(f'{root}/{root}.csv', index=False)
        print(f'{root} done')
    

def add_industry_tg(companies, for_regex_industry):
    files = os.listdir('Telegram prep')
    files.remove('messages')
    if '.DS_Store' in files:
        files.remove('.DS_Store')
    for file in files:
        df = pd.read_csv(f'Telegram prep/{file}', low_memory=False)
        for industry in industries:
            reg = for_regex_industry[industry]
            reg = ' | '.join(reg)
            in_str = df['message'].str.contains(reg)
            df[industry] = in_str
        df.to_csv(f'Telegram prep/{file}', index=False)
        print(f'{file} done')
        
        
def add_target_tg():
    cols = ['1 мин.', '5 мин.', '10 мин.', '15 мин.', '30 мин.', '1 час', '1 день']
    times = [pd.Timedelta(1, unit='min'), pd.Timedelta(5, unit='min'), pd.Timedelta(10, unit='min'),
            pd.Timedelta(15, unit='min'), pd.Timedelta(30, unit='min'), pd.Timedelta(1, unit='hour'), pd.Timedelta(1, unit='day')]
    deltas = {i:j for i, j in zip(cols, times)}
    comp = [com for com in companies]
    files = os.listdir(f'Telegram prep')
    if '.DS_Store' in files:
        files.remove('.DS_Store')
    for file in files:
        df = pd.read_csv(f'Telegram prep/{file}')
        if 'timestamp' in df.columns:
            df.rename(columns={'timestamp':'date'})
        df = df[df['date'] != 'No time']
        df['date'] = pd.to_datetime(df['date'])
        df.drop_duplicates(inplace=True)
        df.sort_values('date', ignore_index=True, inplace=True)

        for com in comp:
            price = pd.read_csv(f'Стоимость акций/1 мин./{com}.csv')
            price['timestamp'] = pd.to_datetime(price['timestamp'])
            price.rename(columns={'timestamp':'date'}, inplace=True)
            price.sort_values('date', ignore_index=True, inplace=True)
            for col in cols:
                df['date'] += deltas[col]
                df = pd.merge_asof(df, price, on='date', direction='forward')
                df.rename(columns={'close': f'{col} {com} close'}, inplace=True)
                df['date'] -= deltas[col]
            print(f'{com} готов')
        df.to_csv(f'Telegram prep/{file[:-13]}.csv', index=False)

def add_target_bin_tg():
    cols = ['1 мин.', '5 мин.', '10 мин.', '15 мин.', '30 мин.', '1 час', '1 день']
    times = [pd.Timedelta(1, unit='min'), pd.Timedelta(5, unit='min'), pd.Timedelta(10, unit='min'),
            pd.Timedelta(15, unit='min'), pd.Timedelta(30, unit='min'), pd.Timedelta(1, unit='hour'), pd.Timedelta(1, unit='day')]
    deltas = {i:j for i, j in zip(cols, times)}
    comp = [com for com in companies]
    files = os.listdir(f'Telegram prep')
    
    if '.DS_Store' in files:
        files.remove('.DS_Store')
    if 'messages' in files:
        files.remove('messages')
    
    for file in files:
        df = pd.read_csv(f'Telegram prep/{file}')
        df['date'] = pd.to_datetime(df['date']) 

        for com in comp:
            price = pd.read_csv(f'Стоимость акций/1 мин./{com}.csv')
            price['timestamp'] = pd.to_datetime(price['timestamp'])
            price.rename(columns={'timestamp':'date'}, inplace=True)
            price.sort_values('date', ignore_index=True, inplace=True)
            prev_price = pd.merge_asof(df, price, on='date', direction='backward')['close']
            for col in cols:
                df['date'] += deltas[col]
                df = pd.merge_asof(df, price, on='date', direction='forward')
                df['close'] = prev_price < df['close']
                df.rename(columns={'close': f'{col} {com} close_bin'}, inplace=True)
                df['date'] -= deltas[col]
            print(f'{com} готов')
        df.to_csv(f'Telegram prep/{file[:-13]}.csv', index=False)

 
 
def add_target(root):
    cols = ['1 мин.', '5 мин.', '10 мин.', '15 мин.', '30 мин.', '1 час', '1 день']
    times = [pd.Timedelta(1, unit='min'), pd.Timedelta(5, unit='min'), pd.Timedelta(10, unit='min'),
            pd.Timedelta(15, unit='min'), pd.Timedelta(30, unit='min'), pd.Timedelta(1, unit='hour'), pd.Timedelta(1, unit='day')]
    deltas = {i:j for i, j in zip(cols, times)}
    comp = [com for com in companies]
    files = os.listdir(f'{root}/prep_cites')
    if '.DS_Store' in files:
        files.remove('.DS_Store')
    df = pd.DataFrame()
    for file in files:
        tmp = pd.read_csv(f'{root}/prep_cites/{file}')
        if 'timestamp' in tmp.columns:
            tmp.rename(columns={'timestamp':'date'})
        tmp = tmp[tmp['date'] != 'No time']
        tmp['date'] = pd.to_datetime(tmp['date']) 
        df = pd.concat([df, tmp], axis=0)
    df.drop_duplicates(inplace=True)
    df.sort_values('date', ignore_index=True, inplace=True)
    print('file gathered')

    for com in comp:
        price = pd.read_csv(f'Стоимость акций/1 мин./{com}.csv')
        price['timestamp'] = pd.to_datetime(price['timestamp'])
        price.rename(columns={'timestamp':'date'}, inplace=True)
        price.sort_values('date', ignore_index=True, inplace=True)
        for col in cols:
            df['date'] += deltas[col]
            df = pd.merge_asof(df, price, on='date', direction='forward')
            df.rename(columns={'close': f'{col} {com} close'}, inplace=True)
            df['date'] -= deltas[col]
        print(f'{com} готов')
    df.to_csv(f'{root}/{root}.csv', index=False)

def add_target_bin(root):
    cols = ['1 мин.', '5 мин.', '10 мин.', '15 мин.', '30 мин.', '1 час', '1 день']
    times = [pd.Timedelta(1, unit='min'), pd.Timedelta(5, unit='min'), pd.Timedelta(10, unit='min'),
            pd.Timedelta(15, unit='min'), pd.Timedelta(30, unit='min'), pd.Timedelta(1, unit='hour'), pd.Timedelta(1, unit='day')]
    deltas = {i:j for i, j in zip(cols, times)}
    comp = [com for com in companies]
    df = pd.read_csv(f'{root}/{root}.csv')
    df['date'] = pd.to_datetime(df['date']) 

    for com in comp:
        price = pd.read_csv(f'Стоимость акций/1 мин./{com}.csv')
        price['timestamp'] = pd.to_datetime(price['timestamp'])
        price.rename(columns={'timestamp':'date'}, inplace=True)
        price.sort_values('date', ignore_index=True, inplace=True)
        prev_price = pd.merge_asof(df, price, on='date', direction='backward')['close']
        for col in cols:
            df['date'] += deltas[col]
            df = pd.merge_asof(df, price, on='date', direction='forward')
            df['close'] = prev_price < df['close']
            df.rename(columns={'close': f'{col} {com} close_bin'}, inplace=True)
            df['date'] -= deltas[col]
        print(f'{com} готов')
    df.to_csv(f'{root}/{root}.csv', index=False)
    
def get_unparsed_urls(path):
    urls = []
    files = os.listdir(f'{path}/raw_cites')
    files = [f for f in files if f[0] != '.']
    for file in files:
        df = pd.DataFrame(pd.read_pickle(f'{path}/raw_cites/{file}'))
        df['tp'] = df['data_or_ex'].map(lambda x: isinstance(x, tuple))
        df.drop(df[df.tp == True].index, inplace=True)
        urls.extend(df['link'].to_list())
    with open(f'{path}/urls_retry.txt', 'w') as file:
        for url in urls:
            file.write(f'{url}\n')
            
def vedomosti_to_standard_date(folders):
    for folder in folders:
        c = 0
        print(f'{folder} в работе')
        files = os.listdir(f'{folder}/prep_cites')
        if '.DS_Store' in files:
            files.remove('.DS_Store')
        for file in files:
            df = pd.read_csv(f'{folder}/prep_cites/{file}')
            df = df[df['date'] != 'No time']
            df['date'] = pd.to_datetime(df['date'].map(lambda x: x[:19]))
            df.to_csv(f'{folder}/prep_cites/{file}', index=False)
            c += 1
            if c % 100 == 0:
                print(f'{c} из {len(files)} обработано')
                
def ria_to_standard_date(folders):
    for folder in folders:
        c = 0
        print(f'{folder} в работе')
        files = os.listdir(f'{folder}/prep_cites')
        if '.DS_Store' in files:
            files.remove('.DS_Store')
        for file in files:
            df = pd.read_csv(f'{folder}/prep_cites/{file}')
            df = df[df['date'] != 'No timestamp']
            try:
                df['date'] = pd.to_datetime(df['date'], format='%H:%M %d.%m.%Y')
            except:
                pass
            df.to_csv(f'{folder}/prep_cites/{file}', index=False)
            c += 1
            if c % 100 == 0:
                print(f'{c} из {len(files)} обработано')
                
def lenta_to_standard_date(folders):
    months = {'января': '01', 'февраля':'02', 'марта':'03', 'апреля':'04', 'мая':'05', 'июня':'06', 'июля':'07', 'августа':'08', 'сентября':'09',
            'октября':'10', 'ноября':'11', 'декабря':'12'}
    for folder in folders:
        c = 0
        files = os.listdir(f'{folder}/prep_cites')
        if '.DS_Store' in files:
            files.remove('.DS_Store')
        for file in files:
            c += 1
            df = pd.read_csv(f'{folder}/prep_cites/{file}')
            for word, replacement in months.items():
                df['date'] = df['date'].str.replace(word, replacement)
            try:
                df['date'] = pd.to_datetime(df['date'], format='%H:%M, %d %m %Y')
                df.to_csv(f'{folder}/prep_cites/{file}', index=False)
            except:
                pass
            if c%100 == 0:
                print(f'Обработано {c} из {len(files)}')
                
def telegram_to_standard_date():
    files = os.listdir('Telegram prep')
    if '.DS_Store' in files:
        files.remove('.DS_Store')

    for file in files:
        df = pd.read_csv(f'Telegram prep/{file}')
        df['date'] = pd.to_datetime(df['date'].map(lambda x: x[:-6]))
        df.to_csv(f'Telegram prep/{file}', index=False)
        
def komersant_to_standard_date(folders):
    for folder in folders:
        c = 0
        files = os.listdir(f'{folder}/prep_cites')
        if '.DS_Store' in files:
            files.remove('.DS_Store')
        
        for file in files:
            df = pd.read_csv(f'{folder}/prep_cites/{file}')
            df = df[df['date'] != 'No time']
            df['date'] = pd.to_datetime(df['date'].map(lambda x: x[:-6]))
            df.to_csv(f'{folder}/prep_cites/{file}', index=False)
            c += 1
            if c%100 == 0:
                print(f'Обработано {c} из {len(files)}')