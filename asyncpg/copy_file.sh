#!/bin/bash
: <<'END'
Скрипт для многократного копирования файла-шаблона с данными по продажам
END
file="data.csv"
source="/content/drive/MyDrive/datasets"
target_folder="/content/sales"
number_of_copies=10
for (( step=1; step<=$number_of_copies; step++ ))
do
cp "$source/$file" "$target_folder"
sed -i "s/c_/c_$step/" "$target_folder/$file"
mv "$target_folder/$file" "$target_folder/data_$step.csv"
done