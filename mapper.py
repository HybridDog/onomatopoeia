#!/usr/bin/env python3
import os.path
import os
import sys
from PIL import Image, ImageDraw
from map import Map
from blocks import build_block
from constants import *
from util import *
import node_definitions


class Mapper:
    def __init__(self, map):
        self.map = map
        self.cnt = 0
        self.done = set()
        self.set_up_images()

    def set_up_images(self):
        """Generate an image for each node and load the mask"""
        self.node_images = {}
        textures = node_definitions.NODE_TEXTURES
        for name, (texture_top, texture_side) in textures.items():
            top = Image.open(os.path.join("textures", texture_top)).convert("RGBA")
            side = Image.open(os.path.join("textures", texture_side)).convert("RGBA")
            node_name = str.encode(name, "ascii")
            self.node_images[node_name] = build_block(top, side)
        self.mask = Image.open("mask.png").convert("1")

    def drawNode(self, canvas, x, y, z, block, start):
        """Draw the three sides of a single node"""
        canvas.paste(
            block,
            (
                start[0] + NODE_SIZE // 2 * (z - x),
                start[1] + NODE_SIZE // 4 * (x + z - 2 * y),
            ),
            self.mask,
        )

    def drawBlock(self, canvas, bx, by, bz, start):
        """ returns max y of visible node """
        map_block = self.map.getBlock(bx, by, bz)
        maxy = -1
        for y in range(NODES_PER_BLOCK):
            for z in range(NODES_PER_BLOCK):
                for x in range(NODES_PER_BLOCK):
                    p = map_block.get(x, y, z)
                    if p in self.node_images:
                        self.drawNode(
                            canvas,
                            x + bx * NODES_PER_BLOCK,
                            y + by * NODES_PER_BLOCK,
                            z + bz * NODES_PER_BLOCK,
                            self.node_images[p],
                            start,
                        )
                        maxy = max(maxy, y + by * NODES_PER_BLOCK)
        return maxy

    def makeChunk(self, cx, cz):
        maxy = -1
        canvas = Image.new("RGBA", (BLOCK_SIZE, CHUNK_HEIGHT))
        for by in range(-8, 8):
            maxy = max(
                maxy,
                self.drawBlock(
                    canvas,
                    cx,
                    by,
                    cz,
                    (
                        BLOCK_SIZE // 2 * (cx - cz + 1) - NODE_SIZE // 2,
                        BLOCK_SIZE // 4 * (BLOCKS_PER_CHUNK - cz - cx) - NODE_SIZE // 2,
                    ),
                ),
            )
        return canvas, maxy

    def fullMap(self):
        canvas = Image.new("RGBA", (5000, 5000))
        start = (3000, 3000)
        for y in range(-1, 10):
            print(y)
            for z in range(-5, 5):
                for x in range(-5, 5):
                    self.drawBlock(canvas, x, y, z, start)
        canvas.save("map.png")

    def chunks3(self, canvas, x, z, step):
        maxy = -1
        chunk, y = self.makeChunk(x, z)
        maxy = max(maxy, y)
        canvas.paste(chunk, (0, step * BLOCK_SIZE // 2), chunk)
        del chunk
        chunk, y = self.makeChunk(x + 1, z)
        maxy = max(maxy, y)
        canvas.paste(
            chunk, (-BLOCK_SIZE // 2, step * BLOCK_SIZE // 2 + BLOCK_SIZE // 4), chunk
        )
        del chunk
        chunk, y = self.makeChunk(x, z + 1)
        maxy = max(maxy, y)
        canvas.paste(
            chunk, (BLOCK_SIZE // 2, step * BLOCK_SIZE // 2 + BLOCK_SIZE // 4), chunk
        )
        del chunk
        return maxy

    # row = x + z
    # col = z - x
    # x = (row - col) / 2
    # z = (row + col) / 2
    """
    def dummyMakeTile(row, col):
        x, z = gridToCoords(row, col)
        canvas = Image.new("RGBA", (BLOCK_SIZE, 18 * BLOCK_SIZE/2))
        for i in range(-16, 2):
            self.chunks3(canvas, x, z, 16 + i)
        tile = canvas.crop((0, 16 * BLOCK_SIZE/2, BLOCK_SIZE, 18 * BLOCK_SIZE/2))
        del canvas
        return tile
    """

    @staticmethod
    def saveTile(tile, row, col, zoom=5):
        path = os.path.join("data", str(zoom), str(row))
        if not os.path.exists(path):
            os.makedirs(path)
        tile.save(os.path.join(path, "%d.png" % col))

    # assume it's safe to start with (x, z)
    def stupidMakeTiles(self, x, z):
        # TODO:                                  v
        canvas = Image.new("RGBA", (BLOCK_SIZE, 100 * BLOCK_SIZE))
        step = 0
        last = 0
        while True:
            print("tiling %d %d" % (x + step, z + step))
            row, col = coordsToGrid(x + step, z + step)
            y = self.chunks3(canvas, x + step, z + step, step)
            # canvas.save("step_{}.png".format(step))
            if row % 4 == 0:
                tile = canvas.crop((0, last, BLOCK_SIZE, last + BLOCK_SIZE))
                last += BLOCK_SIZE
                self.saveTile(tile, row // 4, col // 2)
                del tile
                self.cnt += 1
            self.done.add((x + step, z + step))
            step += 1
            print("y is %d" % y)
            if y == -1:
                break

    def is_done(self, coord):
        return coord in self.done

    def get_cnt(self):
        return self.cnt


def main():
    map = Map(".")
    mapper = Mapper(map)
    raw_coords = list(map.getCoordinatesToDraw())
    coords = []
    for row, col in raw_coords:
        if row % 4 != 0 or col % 2 != 0:
            continue
        coords.append(gridToCoords(row, col))
    coords.sort()

    for coord in coords:
        if mapper.is_done(coord):
            continue
        print("{0}% done".format(100.0 * mapper.get_cnt() / len(coords)))
        mapper.stupidMakeTiles(*coord)

    """
    step = 0
    for row, col in coords:
        step += 1
        print("[{}%]".format(100.0 * step / len(coords)))
        if row % 4 != 0 or col % 2 != 0:
            continue
        path = os.path.join("data", "5", "{}".format(row / 4 ))
        if not os.path.exists(path):
            os.makedirs(path)
        dummyMakeTile(row, col).save(os.path.join(path, "{}.png".format(col / 2)))
    """

    # zoom 4 ---> 0

    to_join = raw_coords

    for zoom in range(4, -1, -1):
        new_join = set()
        for row, col in to_join:
            if zoom == 4:
                if row % 4 != 0 or col % 2 != 0:
                    continue
                row //= 4
                col //= 2
            if row % 2 == 1:
                row -= 1
            if col % 2 == 1:
                col -= 1
            new_join.add((row, col))
        to_join = new_join

        for row, col in to_join:
            # print("join {} {}".format(row, col))
            R = row // 2
            C = col // 2
            path = os.path.join("data", str(zoom), str(R))
            if not os.path.exists(path):
                os.makedirs(path)
            canvas = Image.new("RGBA", (BLOCK_SIZE, BLOCK_SIZE))
            for dx in range(0, 2):
                for dz in range(0, 2):
                    try:
                        tile = Image.open(
                            os.path.join(
                                "data",
                                str(zoom + 1),
                                str(row + dx),
                                "%d.png" % (col + dz),
                            )
                        ).convert("RGBA")
                    except IOError:
                        tile = Image.new("RGBA", (BLOCK_SIZE, BLOCK_SIZE))
                    tile = tile.resize((BLOCK_SIZE // 2, BLOCK_SIZE // 2))
                    canvas.paste(
                        tile, (dz * BLOCK_SIZE // 2, dx * BLOCK_SIZE // 2), tile
                    )
            canvas.save(os.path.join(path, "%d.png" % C))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
