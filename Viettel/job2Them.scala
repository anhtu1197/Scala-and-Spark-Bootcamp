data.groupBy("CALLING_NUMBER", "CALLED_NUMBER").agg(count("CELL_A"))  //so luong cell di qua
 df.groupBy("CELL_A").count().show() // so luong cell giong nhau
  df.groupBy("CELL_A").count().select(count("CELL_A")).show(5) // so luong cell khac nhau
TimeBetweenCalls = ((([TotalHoursWorked]-(sum('TITO Data'[TimeOnCall])/60)))/(count('TITO Data'[EntryDate])-(1*DISTINCTCOUNT('TITO Data'[EntryDate]))))*60
