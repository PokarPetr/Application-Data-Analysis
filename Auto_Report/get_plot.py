import matplotlib.pyplot as plt
import seaborn as sns
import io


class PlotObject:
    def __init__(self, data):
        self.data = data

    @property
    def plot_object(self):
        """
        Create a plot for all metrics
        """
        sns.set()
        fig, axes = plt.subplots(3, 2, figsize=(16, 10), constrained_layout=True)
        fig.suptitle('Statistic by Feed_actions for past 7 days')
        plot_dict = {(0, 0): {'y': 'DAU', 'title': 'Uniq Users'},
                     (0, 1): {'y': 'posts', 'title': 'Uniq Posts'},
                     (1, 0): {'y': 'likes', 'title': 'Likes'},
                     (1, 1): {'y': 'views', 'title': 'Views'},
                     (2, 0): {'y': 'CTR', 'title': 'CTR'},
                     (2, 1): {'y': 'lpu', 'title': 'Likes per user'}
                     }

        for row in range(3):
            for col in range(2):
                sns.lineplot(ax=axes[row, col], data=self.data, x='date', y=plot_dict[(row, col)]['y'])
                axes[row, col].set_title(plot_dict[(row, col)]['title'])
                axes[row, col].set(xlabel=None)
                axes[row, col].set(ylabel=None)
                for ind, label in enumerate(axes[row, col].get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

        plot = io.BytesIO()
        plt.savefig(plot)
        plot.name = 'feed_stat.png'
        plot.seek(0)
        plt.close()
        return plot
