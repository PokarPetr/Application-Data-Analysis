class Query:
    """
    Create a database query with all the metrics we are interested in.

    Создаем запрос к базе данных  со всеми интересующими нас метриками.
    """

    QUERY_FEED = """
            SELECT 
              toDate(time) as date
            , uniqExact(user_id)  as DAU
            , countIf(user_id, action='like') as likes
            , countIf(user_id, action='view') as views 
            , 100 * likes / views as CTR
            , likes / DAU as lpu
            , uniqExact(post_id)  as posts
            , likes + views as events
            FROM simulator_20230120.feed_actions
            WHERE toDate(time) between today() - 8 and today() - 1
            GROUP BY date
            ORDER BY date
    """