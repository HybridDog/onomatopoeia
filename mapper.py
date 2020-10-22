#!/usr/bin/env python3
import os.path
import os
import sys
from PIL import Image, ImageDraw
from map import Map
from blocks import build_block
from constants import *
from util import *

# name: (top, side)
textures = {
    "beds:bed_bottom": ("beds_bed_top_bottom.png", "beds_bed_side_bottom_r.png"),
    "beds:bed_top": ("beds_bed_top_top.png", "beds_bed_side_top_r.png"),
    "beds:fancy_bed_bottom": ("beds_bed_top1.png", "beds_bed_side1.png"),
    "beds:fancy_bed_top": ("beds_bed_top2.png", "beds_bed_side2.png"),
    "bones:bones": ("bones_top.png", "bones_side.png"),
    "carts:brakerail": ("carts_rail_straight_brk.png", "carts_rail_t_junction_brk.png"),
    "carts:powerrail": ("carts_rail_straight_pwr.png", "carts_rail_t_junction_pwr.png"),
    "carts:rail": ("carts_rail_straight.png", "carts_rail_t_junction.png"),
    "default:acacia_bush_leaves": ("default_acacia_leaves_simple.png", "default_acacia_leaves_simple.png"),
    "default:acacia_bush_sapling": ("default_acacia_bush_sapling.png", "default_acacia_bush_sapling.png"),
    "default:acacia_bush_stem": ("default_acacia_bush_stem.png", "default_acacia_bush_stem.png"),
    "default:acacia_leaves": ("default_acacia_leaves.png", "default_acacia_leaves.png"),
    "default:acacia_sapling": ("default_acacia_sapling.png", "default_acacia_sapling.png"),
    "default:acacia_tree": ("default_acacia_tree_top.png", "default_acacia_tree.png"),
    "default:acacia_wood": ("default_acacia_wood.png", "default_acacia_wood.png"),
    "default:apple": ("default_apple.png", "default_apple.png"),
    "default:aspen_leaves": ("default_aspen_leaves.png", "default_aspen_leaves.png"),
    "default:aspen_sapling": ("default_aspen_sapling.png", "default_aspen_sapling.png"),
    "default:aspen_tree": ("default_aspen_tree_top.png", "default_aspen_tree.png"),
    "default:aspen_wood": ("default_aspen_wood.png", "default_aspen_wood.png"),
    "default:bookshelf": ("default_wood.png", "default_wood.png"),
    "default:brick": ("default_brick.png", "default_brick.png"),
    "default:bronzeblock": ("default_bronze_block.png", "default_bronze_block.png"),
    "default:bush_leaves": ("default_leaves_simple.png", "default_leaves_simple.png"),
    "default:bush_sapling": ("default_bush_sapling.png", "default_bush_sapling.png"),
    "default:bush_stem": ("default_bush_stem.png", "default_bush_stem.png"),
    "default:cactus": ("default_cactus_top.png", "default_cactus_side.png"),
    "default:chest": ("default_chest_top.png", "default_chest_side.png"),
    "default:chest_locked": ("default_chest_top.png", "default_chest_side.png"),
    "default:chest_locked_open": ("default_chest_top.png", "default_chest_side.png"),
    "default:chest_open": ("default_chest_top.png", "default_chest_side.png"),
    "default:clay": ("default_clay.png", "default_clay.png"),
    "default:cloud": ("default_cloud.png", "default_cloud.png"),
    "default:coalblock": ("default_coal_block.png", "default_coal_block.png"),
    "default:cobble": ("default_cobble.png", "default_cobble.png"),
    "default:copperblock": ("default_copper_block.png", "default_copper_block.png"),
    "default:coral_brown": ("default_coral_brown.png", "default_coral_brown.png"),
    "default:coral_orange": ("default_coral_orange.png", "default_coral_orange.png"),
    "default:coral_skeleton": ("default_coral_skeleton.png", "default_coral_skeleton.png"),
    "default:desert_cobble": ("default_desert_cobble.png", "default_desert_cobble.png"),
    "default:desert_sand": ("default_desert_sand.png", "default_desert_sand.png"),
    "default:desert_sandstone_block": ("default_desert_sandstone_block.png", "default_desert_sandstone_block.png"),
    "default:desert_sandstone_brick": ("default_desert_sandstone_brick.png", "default_desert_sandstone_brick.png"),
    "default:desert_sandstone": ("default_desert_sandstone.png", "default_desert_sandstone.png"),
    "default:desert_stone_block": ("default_desert_stone_block.png", "default_desert_stone_block.png"),
    "default:desert_stonebrick": ("default_desert_stone_brick.png", "default_desert_stone_brick.png"),
    "default:desert_stone": ("default_desert_stone.png", "default_desert_stone.png"),
    "default:diamondblock": ("default_diamond_block.png", "default_diamond_block.png"),
    "default:dirt": ("default_dirt.png", "default_dirt.png"),
    "default:dirt_with_dry_grass": ("default_dry_grass.png", "default_dirt.png"),
    "default:dirt_with_grass": ("default_grass.png", "default_dirt.png"),
    "default:dirt_with_grass_footsteps": ("default_grass.png", "default_dirt.png"),
    "default:dirt_with_rainforest_litter": ("default_rainforest_litter.png", "default_dirt.png"),
    "default:dirt_with_snow": ("default_snow.png", "default_dirt.png"),
    "default:dry_grass_1": ("default_dry_grass_1.png", "default_dry_grass_1.png"),
    "default:dry_grass_2": ("default_dry_grass_2.png", "default_dry_grass_2.png"),
    "default:dry_grass_3": ("default_dry_grass_3.png", "default_dry_grass_3.png"),
    "default:dry_grass_4": ("default_dry_grass_4.png", "default_dry_grass_4.png"),
    "default:dry_grass_5": ("default_dry_grass_5.png", "default_dry_grass_5.png"),
    "default:dry_shrub": ("default_dry_shrub.png", "default_dry_shrub.png"),
    "default:fence_acacia_wood": ("default_fence_acacia_wood.png", "default_fence_acacia_wood.png"),
    "default:fence_aspen_wood": ("default_fence_aspen_wood.png", "default_fence_aspen_wood.png"),
    "default:fence_junglewood": ("default_fence_junglewood.png", "default_fence_junglewood.png"),
    "default:fence_pine_wood": ("default_fence_pine_wood.png", "default_fence_pine_wood.png"),
    "default:fence_wood": ("default_fence_wood.png", "default_fence_wood.png"),
    "default:furnace_active": ("default_furnace_top.png", "default_furnace_side.png"),
    "default:furnace": ("default_furnace_top.png", "default_furnace_side.png"),
    "default:glass": ("default_glass.png", "default_glass_detail.png"),
    "default:goldblock": ("default_gold_block.png", "default_gold_block.png"),
    "default:grass_1": ("default_grass_1.png", "default_grass_1.png"),
    "default:grass_2": ("default_grass_2.png", "default_grass_2.png"),
    "default:grass_3": ("default_grass_3.png", "default_grass_3.png"),
    "default:grass_4": ("default_grass_4.png", "default_grass_4.png"),
    "default:grass_5": ("default_grass_5.png", "default_grass_5.png"),
    "default:gravel": ("default_gravel.png", "default_gravel.png"),
    "default:ice": ("default_ice.png", "default_ice.png"),
    "default:junglegrass": ("default_junglegrass.png", "default_junglegrass.png"),
    "default:jungleleaves": ("default_jungleleaves.png", "default_jungleleaves.png"),
    "default:junglesapling": ("default_junglesapling.png", "default_junglesapling.png"),
    "default:jungletree": ("default_jungletree_top.png", "default_jungletree.png"),
    "default:junglewood": ("default_junglewood.png", "default_junglewood.png"),
    "default:ladder_steel": ("default_ladder_steel.png", "default_ladder_steel.png"),
    "default:ladder_wood": ("default_ladder_wood.png", "default_ladder_wood.png"),
    "default:lava_flowing": ("default_lava.png", "default_lava.png"),
    "default:lava_source": ("default_lava.png", "default_lava.png"),
    "default:leaves": ("default_leaves.png", "default_leaves.png"),
    "default:mese": ("default_mese_block.png", "default_mese_block.png"),
    "default:meselamp": ("default_meselamp.png", "default_meselamp.png"),
    "default:mese_post_light": ("default_mese_post_light_top.png", "default_mese_post_light_side_dark.png"),
    "default:mossycobble": ("default_mossycobble.png", "default_mossycobble.png"),
    "default:obsidian_block": ("default_obsidian_block.png", "default_obsidian_block.png"),
    "default:obsidianbrick": ("default_obsidian_brick.png", "default_obsidian_brick.png"),
    "default:obsidian": ("default_obsidian.png", "default_obsidian.png"),
    "default:obsidian_glass": ("default_obsidian_glass.png", "default_obsidian_glass_detail.png"),
    "default:papyrus": ("default_papyrus.png", "default_papyrus.png"),
    "default:pine_needles": ("default_pine_needles.png", "default_pine_needles.png"),
    "default:pine_sapling": ("default_pine_sapling.png", "default_pine_sapling.png"),
    "default:pine_tree": ("default_pine_tree_top.png", "default_pine_tree.png"),
    "default:pine_wood": ("default_pine_wood.png", "default_pine_wood.png"),
    "default:river_water_flowing": ("default_river_water.png", "default_river_water.png"),
    "default:river_water_source": ("default_river_water.png", "default_river_water.png"),
    "default:sand": ("default_sand.png", "default_sand.png"),
    "default:sandstone_block": ("default_sandstone_block.png", "default_sandstone_block.png"),
    "default:sandstonebrick": ("default_sandstone_brick.png", "default_sandstone_brick.png"),
    "default:sandstone": ("default_sandstone.png", "default_sandstone.png"),
    "default:sand_with_kelp": ("default_sand.png", "default_sand.png"),
    "default:sapling": ("default_sapling.png", "default_sapling.png"),
    "default:sign_wall_steel": ("default_sign_wall_steel.png", "default_sign_wall_steel.png"),
    "default:sign_wall_wood": ("default_sign_wall_wood.png", "default_sign_wall_wood.png"),
    "default:silver_sand": ("default_silver_sand.png", "default_silver_sand.png"),
    "default:silver_sandstone_block": ("default_silver_sandstone_block.png", "default_silver_sandstone_block.png"),
    "default:silver_sandstone_brick": ("default_silver_sandstone_brick.png", "default_silver_sandstone_brick.png"),
    "default:silver_sandstone": ("default_silver_sandstone.png", "default_silver_sandstone.png"),
    "default:snowblock": ("default_snow.png", "default_snow.png"),
    "default:snow": ("default_snow.png", "default_snow.png"),
    "default:steelblock": ("default_steel_block.png", "default_steel_block.png"),
    "default:stone_block": ("default_stone_block.png", "default_stone_block.png"),
    "default:stonebrick": ("default_stone_brick.png", "default_stone_brick.png"),
    "default:stone": ("default_stone.png", "default_stone.png"),
    "default:stone_with_coal": ("default_stone.png", "default_stone.png"),
    "default:stone_with_copper": ("default_stone.png", "default_stone.png"),
    "default:stone_with_diamond": ("default_stone.png", "default_stone.png"),
    "default:stone_with_gold": ("default_stone.png", "default_stone.png"),
    "default:stone_with_iron": ("default_stone.png", "default_stone.png"),
    "default:stone_with_mese": ("default_stone.png", "default_stone.png"),
    "default:stone_with_tin": ("default_stone.png", "default_stone.png"),
    "default:tinblock": ("default_tin_block.png", "default_tin_block.png"),
    "default:torch_ceiling": ("_torch.png", "_torch.png"),
    "default:torch": ("_torch.png", "_torch.png"),
    "default:torch_wall": ("_torch.png", "_torch.png"),
    "default:tree": ("default_tree_top.png", "default_tree.png"),
    "default:water_flowing": ("default_water.png", "default_water.png"),
    "default:water_source": ("default_water.png", "default_water.png"),
    "default:wood": ("default_wood.png", "default_wood.png"),
    "doors:door_glass_a": ("doors_door_glass.png", "doors_door_glass.png"),
    "doors:door_glass_b": ("doors_door_glass.png", "doors_door_glass.png"),
    "doors:door_obsidian_glass_a": ("doors_door_obsidian_glass.png", "doors_door_obsidian_glass.png"),
    "doors:door_obsidian_glass_b": ("doors_door_obsidian_glass.png", "doors_door_obsidian_glass.png"),
    "doors:door_steel_a": ("doors_door_steel.png", "doors_door_steel.png"),
    "doors:door_steel_b": ("doors_door_steel.png", "doors_door_steel.png"),
    "doors:door_wood_a": ("doors_door_wood.png", "doors_door_wood.png"),
    "doors:door_wood_b": ("doors_door_wood.png", "doors_door_wood.png"),
    "doors:gate_acacia_wood_closed": ("default_acacia_wood.png", "default_acacia_wood.png"),
    "doors:gate_acacia_wood_open": ("default_acacia_wood.png", "default_acacia_wood.png"),
    "doors:gate_aspen_wood_closed": ("default_aspen_wood.png", "default_aspen_wood.png"),
    "doors:gate_aspen_wood_open": ("default_aspen_wood.png", "default_aspen_wood.png"),
    "doors:gate_junglewood_closed": ("default_junglewood.png", "default_junglewood.png"),
    "doors:gate_junglewood_open": ("default_junglewood.png", "default_junglewood.png"),
    "doors:gate_pine_wood_closed": ("default_pine_wood.png", "default_pine_wood.png"),
    "doors:gate_pine_wood_open": ("default_pine_wood.png", "default_pine_wood.png"),
    "doors:gate_wood_closed": ("default_wood.png", "default_wood.png"),
    "doors:gate_wood_open": ("default_wood.png", "default_wood.png"),
    "doors:hidden": ("doors_blank.png", "doors_blank.png"),
    "doors:trapdoor": ("doors_trapdoor.png", "doors_trapdoor_side.png"),
    "doors:trapdoor_open": ("doors_trapdoor_side.png", "doors_trapdoor_side.png"),
    "doors:trapdoor_steel": ("doors_trapdoor_steel.png", "doors_trapdoor_steel_side.png"),
    "doors:trapdoor_steel_open": ("doors_trapdoor_steel_side.png", "doors_trapdoor_steel_side.png"),
    "farming:cotton_1": ("farming_cotton_1.png", "farming_cotton_1.png"),
    "farming:cotton_2": ("farming_cotton_2.png", "farming_cotton_2.png"),
    "farming:cotton_3": ("farming_cotton_3.png", "farming_cotton_3.png"),
    "farming:cotton_4": ("farming_cotton_4.png", "farming_cotton_4.png"),
    "farming:cotton_5": ("farming_cotton_5.png", "farming_cotton_5.png"),
    "farming:cotton_6": ("farming_cotton_6.png", "farming_cotton_6.png"),
    "farming:cotton_7": ("farming_cotton_7.png", "farming_cotton_7.png"),
    "farming:cotton_8": ("farming_cotton_8.png", "farming_cotton_8.png"),
    "farming:desert_sand_soil": ("farming_desert_sand_soil.png", "default_desert_sand.png"),
    "farming:desert_sand_soil_wet": ("farming_desert_sand_soil_wet.png", "farming_desert_sand_soil_wet_side.png"),
    "farming:seed_cotton": ("farming_cotton_seed.png", "farming_cotton_seed.png"),
    "farming:seed_wheat": ("farming_wheat_seed.png", "farming_wheat_seed.png"),
    "farming:soil": ("default_dirt.png", "default_dirt.png"),
    "farming:soil_wet": ("default_dirt.png", "default_dirt.png"),
    "farming:straw": ("farming_straw.png", "farming_straw.png"),
    "farming:wheat_1": ("farming_wheat_1.png", "farming_wheat_1.png"),
    "farming:wheat_2": ("farming_wheat_2.png", "farming_wheat_2.png"),
    "farming:wheat_3": ("farming_wheat_3.png", "farming_wheat_3.png"),
    "farming:wheat_4": ("farming_wheat_4.png", "farming_wheat_4.png"),
    "farming:wheat_5": ("farming_wheat_5.png", "farming_wheat_5.png"),
    "farming:wheat_6": ("farming_wheat_6.png", "farming_wheat_6.png"),
    "farming:wheat_7": ("farming_wheat_7.png", "farming_wheat_7.png"),
    "farming:wheat_8": ("farming_wheat_8.png", "farming_wheat_8.png"),
    "fire:basic_flame": ("_fire.png", "_fire.png"),
    "fire:permanent_flame": ("_fire.png", "_fire.png"),
    "flowers:chrysanthemum_green": ("flowers_chrysanthemum_green.png", "flowers_chrysanthemum_green.png"),
    "flowers:dandelion_white": ("flowers_dandelion_white.png", "flowers_dandelion_white.png"),
    "flowers:dandelion_yellow": ("flowers_dandelion_yellow.png", "flowers_dandelion_yellow.png"),
    "flowers:geranium": ("flowers_geranium.png", "flowers_geranium.png"),
    "flowers:mushroom_brown": ("flowers_mushroom_brown.png", "flowers_mushroom_brown.png"),
    "flowers:mushroom_red": ("flowers_mushroom_red.png", "flowers_mushroom_red.png"),
    "flowers:rose": ("flowers_rose.png", "flowers_rose.png"),
    "flowers:tulip_black": ("flowers_tulip_black.png", "flowers_tulip_black.png"),
    "flowers:tulip": ("flowers_tulip.png", "flowers_tulip.png"),
    "flowers:viola": ("flowers_viola.png", "flowers_viola.png"),
    "flowers:waterlily": ("flowers_waterlily.png", "flowers_waterlily_bottom.png"),
    "stairs:slab_acacia_wood": ("default_acacia_wood.png", "default_acacia_wood.png"),
    "stairs:slab_aspen_wood": ("default_aspen_wood.png", "default_aspen_wood.png"),
    "stairs:slab_brick": ("default_brick.png", "default_brick.png"),
    "stairs:slab_bronzeblock": ("default_bronze_block.png", "default_bronze_block.png"),
    "stairs:slab_cobble": ("default_cobble.png", "default_cobble.png"),
    "stairs:slab_copperblock": ("default_copper_block.png", "default_copper_block.png"),
    "stairs:slab_desert_cobble": ("default_desert_cobble.png", "default_desert_cobble.png"),
    "stairs:slab_desert_sandstone_block": ("default_desert_sandstone_block.png", "default_desert_sandstone_block.png"),
    "stairs:slab_desert_sandstone_brick": ("default_desert_sandstone_brick.png", "default_desert_sandstone_brick.png"),
    "stairs:slab_desert_sandstone": ("default_desert_sandstone.png", "default_desert_sandstone.png"),
    "stairs:slab_desert_stone_block": ("default_desert_stone_block.png", "default_desert_stone_block.png"),
    "stairs:slab_desert_stonebrick": ("default_desert_stone_brick.png", "default_desert_stone_brick.png"),
    "stairs:slab_desert_stone": ("default_desert_stone.png", "default_desert_stone.png"),
    "stairs:slab_goldblock": ("default_gold_block.png", "default_gold_block.png"),
    "stairs:slab_ice": ("default_ice.png", "default_ice.png"),
    "stairs:slab_junglewood": ("default_junglewood.png", "default_junglewood.png"),
    "stairs:slab_mossycobble": ("default_mossycobble.png", "default_mossycobble.png"),
    "stairs:slab_obsidian_block": ("default_obsidian_block.png", "default_obsidian_block.png"),
    "stairs:slab_obsidianbrick": ("default_obsidian_brick.png", "default_obsidian_brick.png"),
    "stairs:slab_obsidian": ("default_obsidian.png", "default_obsidian.png"),
    "stairs:slab_pine_wood": ("default_pine_wood.png", "default_pine_wood.png"),
    "stairs:slab_sandstone_block": ("default_sandstone_block.png", "default_sandstone_block.png"),
    "stairs:slab_sandstonebrick": ("default_sandstone_brick.png", "default_sandstone_brick.png"),
    "stairs:slab_sandstone": ("default_sandstone.png", "default_sandstone.png"),
    "stairs:slab_silver_sandstone_block": ("default_silver_sandstone_block.png", "default_silver_sandstone_block.png"),
    "stairs:slab_silver_sandstone_brick": ("default_silver_sandstone_brick.png", "default_silver_sandstone_brick.png"),
    "stairs:slab_silver_sandstone": ("default_silver_sandstone.png", "default_silver_sandstone.png"),
    "stairs:slab_snowblock": ("default_snow.png", "default_snow.png"),
    "stairs:slab_steelblock": ("default_steel_block.png", "default_steel_block.png"),
    "stairs:slab_stone_block": ("default_stone_block.png", "default_stone_block.png"),
    "stairs:slab_stonebrick": ("default_stone_brick.png", "default_stone_brick.png"),
    "stairs:slab_stone": ("default_stone.png", "default_stone.png"),
    "stairs:slab_straw": ("farming_straw.png", "farming_straw.png"),
    "stairs:slab_tinblock": ("default_tin_block.png", "default_tin_block.png"),
    "stairs:slab_wood": ("default_wood.png", "default_wood.png"),
    "stairs:stair_acacia_wood": ("default_acacia_wood.png", "default_acacia_wood.png"),
    "stairs:stair_aspen_wood": ("default_aspen_wood.png", "default_aspen_wood.png"),
    "stairs:stair_brick": ("default_brick.png", "default_brick.png"),
    "stairs:stair_bronzeblock": ("default_bronze_block.png", "default_bronze_block.png"),
    "stairs:stair_cobble": ("default_cobble.png", "default_cobble.png"),
    "stairs:stair_copperblock": ("default_copper_block.png", "default_copper_block.png"),
    "stairs:stair_desert_cobble": ("default_desert_cobble.png", "default_desert_cobble.png"),
    "stairs:stair_desert_sandstone_block": ("default_desert_sandstone_block.png", "default_desert_sandstone_block.png"),
    "stairs:stair_desert_sandstone_brick": ("default_desert_sandstone_brick.png", "default_desert_sandstone_brick.png"),
    "stairs:stair_desert_sandstone": ("default_desert_sandstone.png", "default_desert_sandstone.png"),
    "stairs:stair_desert_stone_block": ("default_desert_stone_block.png", "default_desert_stone_block.png"),
    "stairs:stair_desert_stonebrick": ("default_desert_stone_brick.png", "default_desert_stone_brick.png"),
    "stairs:stair_desert_stone": ("default_desert_stone.png", "default_desert_stone.png"),
    "stairs:stair_goldblock": ("default_gold_block.png", "default_gold_block.png"),
    "stairs:stair_ice": ("default_ice.png", "default_ice.png"),
    "stairs:stair_inner_acacia_wood": ("default_acacia_wood.png", "default_acacia_wood.png"),
    "stairs:stair_inner_aspen_wood": ("default_aspen_wood.png", "default_aspen_wood.png"),
    "stairs:stair_inner_brick": ("default_brick.png", "default_brick.png"),
    "stairs:stair_inner_bronzeblock": ("default_bronze_block.png", "default_bronze_block.png"),
    "stairs:stair_inner_cobble": ("default_cobble.png", "default_cobble.png"),
    "stairs:stair_inner_copperblock": ("default_copper_block.png", "default_copper_block.png"),
    "stairs:stair_inner_desert_cobble": ("default_desert_cobble.png", "default_desert_cobble.png"),
    "stairs:stair_inner_desert_sandstone_block": ("default_desert_sandstone_block.png", "default_desert_sandstone_block.png"),
    "stairs:stair_inner_desert_sandstone_brick": ("default_desert_sandstone_brick.png", "default_desert_sandstone_brick.png"),
    "stairs:stair_inner_desert_sandstone": ("default_desert_sandstone.png", "default_desert_sandstone.png"),
    "stairs:stair_inner_desert_stone_block": ("default_desert_stone_block.png", "default_desert_stone_block.png"),
    "stairs:stair_inner_desert_stonebrick": ("default_desert_stone_brick.png", "default_desert_stone_brick.png"),
    "stairs:stair_inner_desert_stone": ("default_desert_stone.png", "default_desert_stone.png"),
    "stairs:stair_inner_goldblock": ("default_gold_block.png", "default_gold_block.png"),
    "stairs:stair_inner_ice": ("default_ice.png", "default_ice.png"),
    "stairs:stair_inner_junglewood": ("default_junglewood.png", "default_junglewood.png"),
    "stairs:stair_inner_mossycobble": ("default_mossycobble.png", "default_mossycobble.png"),
    "stairs:stair_inner_obsidian_block": ("default_obsidian_block.png", "default_obsidian_block.png"),
    "stairs:stair_inner_obsidianbrick": ("default_obsidian_brick.png", "default_obsidian_brick.png"),
    "stairs:stair_inner_obsidian": ("default_obsidian.png", "default_obsidian.png"),
    "stairs:stair_inner_pine_wood": ("default_pine_wood.png", "default_pine_wood.png"),
    "stairs:stair_inner_sandstone_block": ("default_sandstone_block.png", "default_sandstone_block.png"),
    "stairs:stair_inner_sandstonebrick": ("default_sandstone_brick.png", "default_sandstone_brick.png"),
    "stairs:stair_inner_sandstone": ("default_sandstone.png", "default_sandstone.png"),
    "stairs:stair_inner_silver_sandstone_block": ("default_silver_sandstone_block.png", "default_silver_sandstone_block.png"),
    "stairs:stair_inner_silver_sandstone_brick": ("default_silver_sandstone_brick.png", "default_silver_sandstone_brick.png"),
    "stairs:stair_inner_silver_sandstone": ("default_silver_sandstone.png", "default_silver_sandstone.png"),
    "stairs:stair_inner_snowblock": ("default_snow.png", "default_snow.png"),
    "stairs:stair_inner_steelblock": ("default_steel_block.png", "default_steel_block.png"),
    "stairs:stair_inner_stone_block": ("default_stone_block.png", "default_stone_block.png"),
    "stairs:stair_inner_stonebrick": ("default_stone_brick.png", "default_stone_brick.png"),
    "stairs:stair_inner_stone": ("default_stone.png", "default_stone.png"),
    "stairs:stair_inner_straw": ("farming_straw.png", "farming_straw.png"),
    "stairs:stair_inner_tinblock": ("default_tin_block.png", "default_tin_block.png"),
    "stairs:stair_inner_wood": ("default_wood.png", "default_wood.png"),
    "stairs:stair_junglewood": ("default_junglewood.png", "default_junglewood.png"),
    "stairs:stair_mossycobble": ("default_mossycobble.png", "default_mossycobble.png"),
    "stairs:stair_obsidian_block": ("default_obsidian_block.png", "default_obsidian_block.png"),
    "stairs:stair_obsidianbrick": ("default_obsidian_brick.png", "default_obsidian_brick.png"),
    "stairs:stair_obsidian": ("default_obsidian.png", "default_obsidian.png"),
    "stairs:stair_outer_acacia_wood": ("default_acacia_wood.png", "default_acacia_wood.png"),
    "stairs:stair_outer_aspen_wood": ("default_aspen_wood.png", "default_aspen_wood.png"),
    "stairs:stair_outer_brick": ("default_brick.png", "default_brick.png"),
    "stairs:stair_outer_bronzeblock": ("default_bronze_block.png", "default_bronze_block.png"),
    "stairs:stair_outer_cobble": ("default_cobble.png", "default_cobble.png"),
    "stairs:stair_outer_copperblock": ("default_copper_block.png", "default_copper_block.png"),
    "stairs:stair_outer_desert_cobble": ("default_desert_cobble.png", "default_desert_cobble.png"),
    "stairs:stair_outer_desert_sandstone_block": ("default_desert_sandstone_block.png", "default_desert_sandstone_block.png"),
    "stairs:stair_outer_desert_sandstone_brick": ("default_desert_sandstone_brick.png", "default_desert_sandstone_brick.png"),
    "stairs:stair_outer_desert_sandstone": ("default_desert_sandstone.png", "default_desert_sandstone.png"),
    "stairs:stair_outer_desert_stone_block": ("default_desert_stone_block.png", "default_desert_stone_block.png"),
    "stairs:stair_outer_desert_stonebrick": ("default_desert_stone_brick.png", "default_desert_stone_brick.png"),
    "stairs:stair_outer_desert_stone": ("default_desert_stone.png", "default_desert_stone.png"),
    "stairs:stair_outer_goldblock": ("default_gold_block.png", "default_gold_block.png"),
    "stairs:stair_outer_ice": ("default_ice.png", "default_ice.png"),
    "stairs:stair_outer_junglewood": ("default_junglewood.png", "default_junglewood.png"),
    "stairs:stair_outer_mossycobble": ("default_mossycobble.png", "default_mossycobble.png"),
    "stairs:stair_outer_obsidian_block": ("default_obsidian_block.png", "default_obsidian_block.png"),
    "stairs:stair_outer_obsidianbrick": ("default_obsidian_brick.png", "default_obsidian_brick.png"),
    "stairs:stair_outer_obsidian": ("default_obsidian.png", "default_obsidian.png"),
    "stairs:stair_outer_pine_wood": ("default_pine_wood.png", "default_pine_wood.png"),
    "stairs:stair_outer_sandstone_block": ("default_sandstone_block.png", "default_sandstone_block.png"),
    "stairs:stair_outer_sandstonebrick": ("default_sandstone_brick.png", "default_sandstone_brick.png"),
    "stairs:stair_outer_sandstone": ("default_sandstone.png", "default_sandstone.png"),
    "stairs:stair_outer_silver_sandstone_block": ("default_silver_sandstone_block.png", "default_silver_sandstone_block.png"),
    "stairs:stair_outer_silver_sandstone_brick": ("default_silver_sandstone_brick.png", "default_silver_sandstone_brick.png"),
    "stairs:stair_outer_silver_sandstone": ("default_silver_sandstone.png", "default_silver_sandstone.png"),
    "stairs:stair_outer_snowblock": ("default_snow.png", "default_snow.png"),
    "stairs:stair_outer_steelblock": ("default_steel_block.png", "default_steel_block.png"),
    "stairs:stair_outer_stone_block": ("default_stone_block.png", "default_stone_block.png"),
    "stairs:stair_outer_stonebrick": ("default_stone_brick.png", "default_stone_brick.png"),
    "stairs:stair_outer_stone": ("default_stone.png", "default_stone.png"),
    "stairs:stair_outer_straw": ("farming_straw.png", "farming_straw.png"),
    "stairs:stair_outer_tinblock": ("default_tin_block.png", "default_tin_block.png"),
    "stairs:stair_outer_wood": ("default_wood.png", "default_wood.png"),
    "stairs:stair_pine_wood": ("default_pine_wood.png", "default_pine_wood.png"),
    "stairs:stair_sandstone_block": ("default_sandstone_block.png", "default_sandstone_block.png"),
    "stairs:stair_sandstonebrick": ("default_sandstone_brick.png", "default_sandstone_brick.png"),
    "stairs:stair_sandstone": ("default_sandstone.png", "default_sandstone.png"),
    "stairs:stair_silver_sandstone_block": ("default_silver_sandstone_block.png", "default_silver_sandstone_block.png"),
    "stairs:stair_silver_sandstone_brick": ("default_silver_sandstone_brick.png", "default_silver_sandstone_brick.png"),
    "stairs:stair_silver_sandstone": ("default_silver_sandstone.png", "default_silver_sandstone.png"),
    "stairs:stair_snowblock": ("default_snow.png", "default_snow.png"),
    "stairs:stair_steelblock": ("default_steel_block.png", "default_steel_block.png"),
    "stairs:stair_stone_block": ("default_stone_block.png", "default_stone_block.png"),
    "stairs:stair_stonebrick": ("default_stone_brick.png", "default_stone_brick.png"),
    "stairs:stair_stone": ("default_stone.png", "default_stone.png"),
    "stairs:stair_straw": ("farming_straw.png", "farming_straw.png"),
    "stairs:stair_tinblock": ("default_tin_block.png", "default_tin_block.png"),
    "stairs:stair_wood": ("default_wood.png", "default_wood.png"),
    "tnt:gunpowder_burning": ("tnt_gunpowder_straight.png", "tnt_gunpowder_straight.png"),
    "tnt:gunpowder": ("tnt_gunpowder_straight.png", "tnt_gunpowder_t_junction.png"),
    "tnt:tnt_burning": ("tnt_top.png", "tnt_side.png"),
    "tnt:tnt": ("tnt_top.png", "tnt_side.png"),
    "vessels:drinking_glass": ("vessels_drinking_glass.png", "vessels_drinking_glass.png"),
    "vessels:glass_bottle": ("vessels_glass_bottle.png", "vessels_glass_bottle.png"),
    "vessels:shelf": ("default_wood.png", "default_wood.png"),
    "vessels:steel_bottle": ("vessels_steel_bottle.png", "vessels_steel_bottle.png"),
    "walls:cobble": ("default_cobble.png", "default_cobble.png"),
    "walls:desertcobble": ("default_desert_cobble.png", "default_desert_cobble.png"),
    "walls:mossycobble": ("default_mossycobble.png", "default_mossycobble.png"),
    "wool:black": ("wool_black.png", "wool_black.png"),
    "wool:blue": ("wool_blue.png", "wool_blue.png"),
    "wool:brown": ("wool_brown.png", "wool_brown.png"),
    "wool:cyan": ("wool_cyan.png", "wool_cyan.png"),
    "wool:dark_green": ("wool_dark_green.png", "wool_dark_green.png"),
    "wool:dark_grey": ("wool_dark_grey.png", "wool_dark_grey.png"),
    "wool:green": ("wool_green.png", "wool_green.png"),
    "wool:grey": ("wool_grey.png", "wool_grey.png"),
    "wool:magenta": ("wool_magenta.png", "wool_magenta.png"),
    "wool:orange": ("wool_orange.png", "wool_orange.png"),
    "wool:pink": ("wool_pink.png", "wool_pink.png"),
    "wool:red": ("wool_red.png", "wool_red.png"),
    "wool:violet": ("wool_violet.png", "wool_violet.png"),
    "wool:white": ("wool_white.png", "wool_white.png"),
    "wool:yellow": ("wool_yellow.png", "wool_yellow.png"),
    "xpanes:bar_flat": ("xpanes_bar_top.png", "xpanes_bar.png"),
    "xpanes:bar": ("xpanes_bar_top.png", "xpanes_bar.png"),
    "xpanes:pane_flat": ("xpanes_white.png", "default_glass.png"),
    "xpanes:pane": ("xpanes_white.png", "default_glass.png")
}

blocks = {}

for name in textures:
    top = Image.open(os.path.join("textures", textures[name][0])).convert("RGBA")
    side = Image.open(os.path.join("textures", textures[name][1])).convert("RGBA")
    block_name = str.encode(name, "ascii")
    blocks[block_name] = build_block(top, side)

mask = Image.open("mask.png").convert("1")

map = Map(".")


def drawNode(canvas, x, y, z, block, start):
    canvas.paste(block, (start[0] + NODE_SIZE//2 * (z - x), start[1] + NODE_SIZE//4 * (x + z - 2 * y)), mask)


def drawBlock(canvas, bx, by, bz, start):
    """ returns max y of visible node """
    map_block = map.getBlock(bx, by, bz)
    maxy = -1
    for y in range(NODES_PER_BLOCK):
        for z in range(NODES_PER_BLOCK):
            for x in range(NODES_PER_BLOCK):
                p = map_block.get(x, y, z)
                if p in blocks:
                    drawNode(canvas, x + bx * NODES_PER_BLOCK, y + by * NODES_PER_BLOCK, z + bz * NODES_PER_BLOCK, blocks[p], start)
                    maxy = max(maxy, y + by * NODES_PER_BLOCK)
    return maxy

cached_chunks = {}

def makeChunk(cx, cz):
    maxy = -1
    canvas = Image.new("RGBA", (BLOCK_SIZE, CHUNK_HEIGHT))
    for by in range(-8, 8):
        maxy = max(maxy, drawBlock(canvas, cx, by, cz, (BLOCK_SIZE//2 * (cx - cz + 1) - NODE_SIZE//2, BLOCK_SIZE//4 * (BLOCKS_PER_CHUNK - cz - cx) - NODE_SIZE//2)))
    return canvas, maxy


def fullMap():
    canvas = Image.new("RGBA", (5000, 5000))
    start = (3000, 3000)
    for y in range(-1, 10):
        print(y)
        for z in range(-5, 5):
            for x in range(-5, 5):
                drawBlock(canvas, x, y, z, start)
    canvas.save("map.png")


def chunks3(canvas, x, z, step):
    maxy = -1
    chunk, y = makeChunk(x, z)
    maxy = max(maxy, y)
    canvas.paste(chunk, (0, step * BLOCK_SIZE//2), chunk)
    del chunk
    chunk, y = makeChunk(x + 1, z)
    maxy = max(maxy, y)
    canvas.paste(chunk, (-BLOCK_SIZE//2, step * BLOCK_SIZE//2 + BLOCK_SIZE//4), chunk)
    del chunk
    chunk, y = makeChunk(x, z + 1)
    maxy = max(maxy, y)
    canvas.paste(chunk, (BLOCK_SIZE//2, step * BLOCK_SIZE//2 + BLOCK_SIZE//4), chunk)
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
        chunks3(canvas, x, z, 16 + i)
    tile = canvas.crop((0, 16 * BLOCK_SIZE/2, BLOCK_SIZE, 18 * BLOCK_SIZE/2))
    del canvas
    return tile
"""


def saveTile(tile, row, col, zoom=5):
    path = os.path.join("data", str(zoom), str(row))
    if not os.path.exists(path):
        os.makedirs(path)
    tile.save(os.path.join(path, "%d.png" % col))

cnt = 0
done = set()

# assume it's safe to start with (x, z)
def stupidMakeTiles(x, z):
    # TODO:                                  v
    canvas = Image.new("RGBA", (BLOCK_SIZE, 100 * BLOCK_SIZE))
    step = 0
    last = 0
    while True:
        print("tiling %d %d" % (x + step, z + step))
        row, col = coordsToGrid(x + step, z + step)
        y = chunks3(canvas, x + step, z + step, step)
        #canvas.save("step_{}.png".format(step))
        if row % 4 == 0:
            tile = canvas.crop((0, last, BLOCK_SIZE, last + BLOCK_SIZE))
            last += BLOCK_SIZE
            saveTile(tile, row // 4, col // 2)
            del tile
            global cnt
            cnt += 1
        done.add((x + step, z + step))
        step += 1
        print("y is %d" % y)
        if y == -1:
            break


raw_coords = list(map.getCoordinatesToDraw())
coords = []
for row, col in raw_coords:
    if row % 4 != 0 or col % 2 != 0:
        continue
    coords.append(gridToCoords(row, col))
coords.sort()

for coord in coords:
    if coord in done:
        continue
    print("{0}% done".format(100.0 * cnt / len(coords)))
    stupidMakeTiles(*coord)

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
        #print("join {} {}".format(row, col))
        R = row // 2
        C = col // 2
        path = os.path.join("data", str(zoom), str(R))
        if not os.path.exists(path):
            os.makedirs(path)
        canvas = Image.new("RGBA", (BLOCK_SIZE, BLOCK_SIZE))
        for dx in range(0, 2):
            for dz in range(0, 2):
                try:
                    tile = Image.open(os.path.join("data", str(zoom + 1), str(row + dx), "%d.png" % (col + dz))).convert("RGBA")
                except IOError:
                    tile = Image.new("RGBA", (BLOCK_SIZE, BLOCK_SIZE))
                tile = tile.resize((BLOCK_SIZE//2, BLOCK_SIZE//2))
                canvas.paste(tile, (dz * BLOCK_SIZE//2, dx * BLOCK_SIZE//2), tile)
        canvas.save(os.path.join(path, "%d.png" % C))
