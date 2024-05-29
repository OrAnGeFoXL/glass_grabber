import functools
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np




def save_plots_to_pdf(n_splits, fname='output.pdf'):
    def decorator(func, *kwargs):
        @functools.wraps(func)
        def wrapper(data, *args, **kwargs):
            # Разделите датасет на n_splits частей
            splits = np.array_split(data, n_splits)

            # Создайте объект для сохранения графиков в PDF файл
            with PdfPages(fname) as pdf:
                for i, split in enumerate(splits):
                    # Создайте новый объект Figure и Axes для каждой части датасета
                    fig, ax = plt.subplots()

                    # Вызовите функцию для построения графика
                    func(split, ax)

                    # Установите заголовок, метки оси и сохраните график в PDF файл
                    ax.set_title(f'Part {i+1}')
                    pdf.savefig(bbox_inches='tight')

                    plt.close(fig)

        return wrapper
    return decorator

