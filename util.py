

def gridToCoords(row, col):
    return (row - col) // 2, (row + col) // 2


def coordsToGrid(x, z):
    return x + z, z - x
