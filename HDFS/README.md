## HDFS

Популярные команды и рассказанно о структуре данных

- /user/wave7_projectb - путь где хранятся данные

```
hdfs dfs -ls /user/wave7_projectb
```
- Назначение прав для группы (**выполняем после изменений в директории!**)
```
hdfs dfs -ls -R /user/wwave7_projectb | awk -v user="$USER" '$3 == user {print $8}' | xargs -I {} hdfs dfs -chmod g+rwx,o+rwx {}
```
#### Cтруктура

- ---- **user**
- --------- **wave7_projectb** - проект команды B
- ----------------- **transactions** - хранятся транзакции после обработки Spark

------------------
#### Дополнительно

- Быстрое удаление данных из сырого слоя
```
hdfs dfs -rm /user/wave7_projectb/transactions/*
```