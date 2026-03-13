import pandas as pd
import pandahouse
import matplotlib.pyplot as plt
import seaborn as sns
import io
import telegram

from airflow.decorators import dag, task
from datetime import datetime, timedelta

sns.set()

#Дефолтные параметры, которые прокидываются в таски
default_args = {
   'owner': 'elena-prihodko',
    'depends_on_past': False,
    'retries': 2, 
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 28),
    }

#Интервал запуска DAG (МСК)
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def prihodko_report_lenta():
    
     #извлекаем данные из таблицы за 7 дней: DAU,просмотры, лайки, CTR, LPU, количество событий и постов
    @task()
    def extract_df(query='Select 1', host='http://clickhouse.lab.karpov.courses:8123', user='student', password='dpo_python_2020'):
        connection = {
            'host': 'http://clickhouse.lab.karpov.courses:8123',
            'database':'simulator_20251220',
            'user':'student',
            'password':'...'
        }
            # запрос        
        query = '''SELECT 
            toDate(time) as date,
            uniqExact(user_id) as DAU,
            countIf(user_id, action = 'view') as views,
            countIf(user_id, action = 'like') as likes,
            likes / views * 100 as ctr,
            views + likes as events,
            uniqExact(post_id) as posts,
            likes/DAU as LPU
            From simulator_20251220.feed_actions
            WHERE toDate(time) between today() - 8 AND today()-1
            GROUP BY date
            ORDER BY date
            '''

        data = pandahouse.read_clickhouse(query, connection=connection)
        return data

    #Создаем отчет по ленте новостей для чата telegram, где даты за вчера и неделю назад:всего событий, DAU,просмотры, лайки, CTR, LPU, количество событий и постов
    @task()
    def feed_report(data):
        msg = '''
        📑 Отчет по ленте новостей за {date} 📑
    Events: {events}
    👤DAU: {users} ({to_users_day_ago:+.2%} к дню назад, {to_users_week_ago:+.2%} к неделе назад)
    ❤️Likes: {likes} ({to_likes_day_ago:+.2%}к дню назад, {to_likes_week_ago:+.2%}к неделе назад)
    👀Views: {views} ({to_views_day_ago:+.2%}к дню назад, {to_views_week_ago:+.2%}к неделе назад)
    🎯CTR: {ctr} ({to_ctr_day_ago:+.2%}к дню назад, {to_ctr_week_ago:+.2%}к неделе назад)
    Posts: {posts} ({to_posts_day_ago:+.2%}к дню назад, {to_posts_week_ago:+.2%}к неделе назад)
    Likes per user: {lpu} ({to_lpu_day_ago:+.2%}к дню назад, {to_lpu_week_ago:+.2%}к неделе назад)
    '''

        today = pd.Timestamp('now') - pd.DateOffset(days=1)
        day_ago = today - pd.DateOffset(days=1)
        week_ago = today - pd.DateOffset(days=7)

        data['date'] = pd.to_datetime(data['date']).dt.date
        data = data.astype({'DAU': int, 'views': int, 'likes': int,'events': int,'posts': int})

        report = msg.format(date= today.date(),
                            events=data[data['date']==today.date()]['events'].iloc[0],
                            users=data[data['date'] == today.date()]['DAU'].iloc[0],
                            to_users_day_ago=(data[data['date'] == today.date()]['DAU'].iloc[0] -
                                            data[data['date'] == day_ago.date()]['DAU'].iloc[0])/
                                            data[data['date'] == day_ago.date()]['DAU'].iloc[0],
                            to_users_week_ago=(data[data['date'] == today.date()]['DAU'].iloc[0] -
                                            data[data['date'] == week_ago.date()]['DAU'].iloc[0])/
                                            data[data['date'] == week_ago.date()]['DAU'].iloc[0],
                            likes=data[data['date'] == today.date()]['likes'].iloc[0],
                            to_likes_day_ago=(data[data['date'] == today.date()]['likes'].iloc[0] -
                                            data[data['date'] == day_ago.date()]['likes'].iloc[0])/
                                            data[data['date'] == day_ago.date()]['likes'].iloc[0],
                            to_likes_week_ago=(data[data['date'] == today.date()]['likes'].iloc[0] -
                                            data[data['date'] == week_ago.date()]['likes'].iloc[0])/
                                            data[data['date'] == week_ago.date()]['likes'].iloc[0],
                            views=data[data['date'] == today.date()]['views'].iloc[0],
                            to_views_day_ago=(data[data['date'] == today.date()]['views'].iloc[0] -
                                            data[data['date'] == day_ago.date()]['views'].iloc[0])/
                                            data[data['date'] == day_ago.date()]['views'].iloc[0],
                            to_views_week_ago=(data[data['date'] == today.date()]['views'].iloc[0] -
                                            data[data['date'] == week_ago.date()]['views'].iloc[0])/
                                            data[data['date'] == week_ago.date()]['views'].iloc[0],
                            ctr=data[data['date'] == today.date()]['ctr'].iloc[0],
                            to_ctr_day_ago=(data[data['date'] == today.date()]['ctr'].iloc[0] -
                                            data[data['date'] == day_ago.date()]['ctr'].iloc[0])/
                                            data[data['date'] == day_ago.date()]['ctr'].iloc[0],
                            to_ctr_week_ago=(data[data['date'] == today.date()]['ctr'].iloc[0] -
                                            data[data['date'] == week_ago.date()]['ctr'].iloc[0])/
                                            data[data['date'] == week_ago.date()]['ctr'].iloc[0],
                            posts=data[data['date'] == today.date()]['posts'].iloc[0],
                            to_posts_day_ago=(data[data['date'] == today.date()]['posts'].iloc[0] -
                                            data[data['date'] == day_ago.date()]['posts'].iloc[0])/
                                            data[data['date'] == day_ago.date()]['posts'].iloc[0],
                            to_posts_week_ago=(data[data['date'] == today.date()]['posts'].iloc[0] -
                                            data[data['date'] == week_ago.date()]['posts'].iloc[0])/
                                            data[data['date'] == week_ago.date()]['posts'].iloc[0],
                            lpu=data[data['date'] == today.date()]['LPU'].iloc[0],
                            to_lpu_day_ago=(data[data['date'] == today.date()]['LPU'].iloc[0] -
                                            data[data['date'] == day_ago.date()]['LPU'].iloc[0])/
                                            data[data['date'] == day_ago.date()]['LPU'].iloc[0],
                            to_lpu_week_ago=(data[data['date'] == today.date()]['LPU'].iloc[0] -
                                            data[data['date'] == week_ago.date()]['LPU'].iloc[0])/
                                            data[data['date'] == week_ago.date()]['LPU'].iloc[0]
                           )
        return report
    #Строим график для отправки в чат telegram
    @task()
    def make_plot(data):
        fig, axes = plt.subplots(2, 2, figsize=(16, 10))        
        fig.suptitle('Статистика по ленте новостей за предыдущие 7 дней')

        plot_dict = {(0, 0): {'y': 'DAU', 'title': 'Уникальные пользователи'},
             (0, 1): {'y': 'views', 'title': 'Views'},
             (1, 0): {'y': 'likes', 'title': 'Likes'},
             (1, 1): {'y': 'ctr', 'title': 'CTR'}
            }

        for i in range(2):
            for j in range(2):
                sns.lineplot(ax=axes[i, j], data=data, x='date', y=plot_dict[(i, j)]['y'])
                axes[i, j].set_title(plot_dict[(i, j)]['title'])
                axes[i, j].set(xlabel=None)
                axes[i, j].set(ylabel=None)
                for ind, label in enumerate(axes[i, j].get_xticklabels()):
                    if ind % 3 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

        plot_object = io.BytesIO()   # создаем объект
        plt.savefig(plot_object)   # сохраняем график в объект
        plot_object.name = 'Feed_status.png'   # присваиваем название объекту
        plot_object.seek(0)   # передвигаем курсор в начало
        plt.close()   # закрываем объект

        return plot_object
    @task()
    def send(report, plot_object, chat_id):
        # инициализируем бота
        my_token = '...' # токен моего бота t.me/Nikitsina_bot
        bot = telegram.Bot(token=my_token) # получаем доступ
        chat_id=...

        bot.sendMessage(chat_id=chat_id, text=report)    # отправляем текстовое сообщение
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)    # отправляем график


    data = extract_df()
    report = feed_report(data)
    plot_object = make_plot(data)
    send(report, plot_object, chat_id=-1002614297220)

prihodko_report_lenta = prihodko_report_lenta()
