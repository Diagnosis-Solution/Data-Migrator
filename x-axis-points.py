import numpy as np


def generate_x_axis(frequency, points):
    delta = 1 / frequency
    np.savetxt('output/{}-{}'.format(frequency, points),
               np.arange(0, points * delta, delta),
               fmt="%s,", newline='')


generate_x_axis(102400, 6400)
