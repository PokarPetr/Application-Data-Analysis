import pandas as pd
from message_template import MSG


class FeedReport:
    def __init__(self, data):
        self.data = data

    @property
    def report(self):
        """
        Create a message.
        The value of the metric for yesterday goes along
        with the value for the previous day and the value for the day a week ago.
        metric values for the previous day and a week ago are shown as
        a percentage of the metric value for the current day

        Создаём сообщение.
        значения метрик за предыдущий день и день неделю назад показаны
        в процентах к значению метрик за текущий день.
        """
        msg = MSG.MSG_TEMPLATE

        today = pd.Timestamp('now') - pd.DateOffset(days=1)
        day_ago = today - pd.DateOffset(days=1)
        week_ago = today - pd.DateOffset(days=7)

        report_feed = msg.format(date=today.date(),
                                 events=self.data[self.data['date'] == today.date()]['events'].iloc[0],

                                 user=self.data[self.data['date'] == today.date()]['DAU'].iloc[0],
                                 to_users_day_ago=(self.data[self.data['date'] == today.date()]['DAU'].iloc[0]
                                                   - self.data[self.data['date'] == day_ago.date()]['DAU'].iloc[0])
                                                  / self.data[self.data['date'] == day_ago.date()]['DAU'].iloc[0],

                                 to_users_week_ago=(self.data[self.data['date'] == today.date()]['DAU'].iloc[0]
                                                    - self.data[self.data['date'] == week_ago.date()]['DAU'].iloc[0])
                                                   / self.data[self.data['date'] == week_ago.date()]['DAU'].iloc[0],

                                 likes=self.data[self.data['date'] == today.date()]['likes'].iloc[0],
                                 to_likes_day_ago=(self.data[self.data['date'] == today.date()]['likes'].iloc[0]
                                                   - self.data[self.data['date'] == day_ago.date()]['likes'].iloc[0])
                                                  / self.data[self.data['date'] == day_ago.date()]['likes'].iloc[0],
                                 to_likes_week_ago=(self.data[self.data['date'] == today.date()]['likes'].iloc[0]
                                                    - self.data[self.data['date'] == week_ago.date()]['likes'].iloc[0])
                                                   / self.data[self.data['date'] == week_ago.date()]['likes'].iloc[0],

                                 views=self.data[self.data['date'] == today.date()]['views'].iloc[0],
                                 to_views_day_ago=(self.data[self.data['date'] == today.date()]['views'].iloc[0]
                                                   - self.data[self.data['date'] == day_ago.date()]['views'].iloc[0])
                                                  / self.data[self.data['date'] == day_ago.date()]['views'].iloc[0],
                                 to_views_week_ago=(self.data[self.data['date'] == today.date()]['views'].iloc[0]
                                                    - self.data[self.data['date'] == week_ago.date()]['views'].iloc[0])
                                                   / self.data[self.data['date'] == week_ago.date()]['views'].iloc[0],

                                 ctr=self.data[self.data['date'] == today.date()]['CTR'].iloc[0],
                                 to_ctr_day_ago=(self.data[self.data['date'] == today.date()]['CTR'].iloc[0]
                                                 - self.data[self.data['date'] == day_ago.date()]['CTR'].iloc[0])
                                                / self.data[self.data['date'] == day_ago.date()]['CTR'].iloc[0],
                                 to_ctr_week_ago=(self.data[self.data['date'] == today.date()]['CTR'].iloc[0]
                                                  - self.data[self.data['date'] == week_ago.date()]['CTR'].iloc[0])
                                                 / self.data[self.data['date'] == week_ago.date()]['CTR'].iloc[0],

                                 posts=self.data[self.data['date'] == today.date()]['posts'].iloc[0],
                                 to_posts_day_ago=(self.data[self.data['date'] == today.date()]['posts'].iloc[0]
                                                   - self.data[self.data['date'] == day_ago.date()]['posts'].iloc[0])
                                                  / self.data[self.data['date'] == day_ago.date()]['posts'].iloc[0],
                                 to_posts_week_ago=(self.data[self.data['date'] == today.date()]['posts'].iloc[0]
                                                    - self.data[self.data['date'] == week_ago.date()]['posts'].iloc[0])
                                                   / self.data[self.data['date'] == week_ago.date()]['posts'].iloc[0],

                                 lpu=self.data[self.data['date'] == today.date()]['lpu'].iloc[0],
                                 to_lpu_day_ago=(self.data[self.data['date'] == today.date()]['lpu'].iloc[0]
                                                 - self.data[self.data['date'] == day_ago.date()]['lpu'].iloc[0])
                                                / self.data[self.data['date'] == day_ago.date()]['lpu'].iloc[0],
                                 to_lpu_week_ago=(self.data[self.data['date'] == today.date()]['lpu'].iloc[0]
                                                  - self.data[self.data['date'] == week_ago.date()]['lpu'].iloc[0])
                                                 / self.data[self.data['date'] == week_ago.date()]['lpu'].iloc[0]
                                 )

        return report_feed



