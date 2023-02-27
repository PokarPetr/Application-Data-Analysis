class MSG:
    """
    Create a message template.
    The value of the metric for yesterday goes along
    with the value for the previous day and the value for the day a week ago.

    Создаём шаблон сообщения.
    Значение метрики за вчерашний день идёт
    вместе со значением за предыдущий день и значением за день неделю назад.
    """
    MSG_TEMPLATE = """
    Report for      {date}
    Events:         {events}
    DAU:            {user} ({to_users_day_ago:+.2%} to day ago, {to_users_week_ago:+.2%} to week ago)
    Likes:          {likes} ({to_likes_day_ago:+.2%} to day ago, {to_likes_week_ago:+.2%} to week ago)
    Views:          {views} ({to_views_day_ago:+.2%} to day ago, {to_views_week_ago:+.2%} to week ago)
    CTR:            {ctr:.2f}% ({to_ctr_day_ago:+.2%} to day ago, {to_ctr_week_ago:+.2%} to week ago)
    Posts:          {posts} ({to_posts_day_ago:+.2%} to day ago, {to_posts_week_ago:+.2%} to week ago)
    Likes per user: {lpu:.2f} ({to_lpu_day_ago:+.2%} to day ago, {to_lpu_week_ago:+.2%} to week ago)
    """
