library(arrow)
library(janitor)
library(dplyr)
library(ggplot2)
library(rnaturalearth)
library(rnaturalearthdata)
library(sf)
library(ggrepel)


# chunk0 <- read.csv("E:/eBirdData/Chunk0.csv") %>% 
#   clean_names()

# names(chunk0)[47]

argentina <- arrow::read_parquet(here::here("data/countrySplits/Argentina/Argentina.parquet"))


argentina %>% 
  head(10) %>% 
  View()

world <- ne_countries(scale = "medium", returnclass = "sf")

argentina_map <- world[world$name == "Argentina",]

argentina %>% 
  head(1000) %>% 
  clean_names() %>% 
  ggplot() +
  geom_sf(data = argentina_map) +
  geom_point(aes(longitude, latitude, color = common_name)) +
  theme(legend.position = "none")

world <- ne_countries(scale = "medium", returnclass = "sf")
northAmerica <- world[world$continent == "North America", ]


usa <- northAmerica[northAmerica$name == "United States"]

bbox_list <- lapply(st_geometry(northAmerica), st_bbox)
bboxs <- bind_rows(bbox_list, .id = "row")

test <- read_parquet("E:/eBirdData/countrySplits/United States.parquet")



allDat <- open_dataset("E:/eBirdData/allData.parquet")

head(allDat, n = 1L) %>% as_tibble() %>% View()

# test <- read.csv("E:/eBirdData/errorDataTest.csv")

hold <- allDat

names(hold)

USCANMEX <- hold %>% 
  filter(COUNTRY %in% c("Canada", "United States", "Mexico"))

test <- USCANMEX %>% collect()

data <- hold %>% 
  clean_names() %>% 
  select(-c("global_unique_identifier", "last_edited_date")) %>% 
  head(100)

data %>% 
  filter(country %in% c("Canada")) %>% 
  ggplot() +
  geom_sf(data = northAmerica) +
  geom_point(aes(longitude, latitude), color = "red") +
  geom_text_repel(aes(longitude, latitude, label = common_name),
                  max.overlaps = 100,
                  force = 2,
                  size = 3) +
  coord_sf(xlim = c(-95, -70), ylim = c(30, 60))

hold %>% 
  clean_names() %>% 
  ggplot() +
  geom_sf(data = world) +
  geom_point(aes(longitude, latitude), color = "red")
