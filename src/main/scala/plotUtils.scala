import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._

object plotUtils {
  
  /**
   * Need this to plot in Zeppelin notebook.
   */
  implicit val render = vegas.render.ShowHTML(s => print("%html " + s))
  
  
  /**
   * Make a convenient and persistent way to set the size of the plots.
   */
  var plot_size_x = 300
  var plot_size_y = 300
    
  def setPlotSize(x:Int, y:Int) {
    plot_size_x = x
    plot_size_y = y
  }
  
  
  /**
   * Transform x-y data into the form needed by the plot function.
   * Vegas can do this internally, but that method doesn't let us put
   * multiple things on the same axes.
   * 
   * data: two arrays of data, X and Y
   * title: the title of this series, for plotting multiple series on the same axes
   */
   def vegasData(data: (Array[Double], Array[Double]), title: String): Array[Map[String,Any]] = {
     if (data._1.length < 2) {
         println("ERROR: vegasData() - not enough data for plotting")
         return Array()
     }

     if (data._1.length != data._2.length) {
         println("ERROR: vegasData() - data arrays must be the same length")
         return Array()
     }

     return data.zipped.toArray.map( x => Map("x"->x._1, "y"->x._2, "title"->title))
  }


  /**
   * A convenient wrapper for a particular style of Vegas plot with logarithmic x and y-axis.
   * This is intended to be called with the result of the vegasData() function above.
   * 
   * The data points (each is  Map) should have the data labeld by "x" and "y",
   * and each point should have an entry for "title" which will be used for
   * associating the data series and labeling them.
   */
  def multiLineLogLogPlot(data: Array[Map[String,Any]], plotname:String = "my plot", title:String = "data series") {
    if (data.length < 2) {
      println("ERROR: multiLineLogPlot() - not enough data for plotting.")
      return
    }
    Vegas(plotname, width=plot_size_x, height=plot_size_y)
      .withData(data)
      .mark(Line)
      .encodeX("x", Quant, scale=Scale(Some(vegas.ScaleType.Log)))
      .encodeY("y", Quant, scale=Scale(Some(vegas.ScaleType.Log)))
      .encodeColor(
         field="title",
         dataType=Nominal,
         legend=Legend(orient="top-left", offset=30, title=title))
      .show
  }

  /**
   * A convenient wrapper for a particular style of Vegas plot with logarithmic y-axis.
   * This is intended to be called with the result of the vegasData() function above.
   * 
   * The data points (each is  Map) should have the data labeld by "x" and "y",
   * and each point should have an entry for "title" which will be used for
   * associating the data series and labeling them.
   */
  def multiLineLogYPlot(data: Array[Map[String,Any]], plotname:String = "my plot", title:String = "data series") {
    if (data.length < 2) {
      println("ERROR: multiLineLogPlot() - not enough data for plotting.")
      return
    }
    Vegas(plotname, width=plot_size_x, height=plot_size_y)
      .withData(data)
      .mark(Line)
      .encodeX("x", Quant, scale=Scale(zero=false))
      .encodeY("y", Quant, scale=Scale(Some(vegas.ScaleType.Log)))
      .encodeColor(
         field="title",
         dataType=Nominal,
         legend=Legend(orient="top-left", offset=30, title=title))
      .show
  }

  /**
   * A convenient wrapper for a particular style of Vegas plot with logarithmic x-axis.
   * This is intended to be called with the result of the vegasData() function above.
   * 
   * The data points (each is  Map) should have the data labeld by "x" and "y",
   * and each point should have an entry for "title" which will be used for
   * associating the data series and labeling them.
   */
  def multiLineLogXPlot(data: Array[Map[String,Any]], plotname:String = "my plot", title:String = "data series") {
    if (data.length < 2) {
      println("ERROR: multiLineLogPlot() - not enough data for plotting.")
      return
    }
    Vegas(plotname, width=plot_size_x, height=plot_size_y)
      .withData(data)
      .mark(Line)
      .encodeX("x", Quant, scale=Scale(Some(vegas.ScaleType.Log)))
      .encodeY("y", Quant, scale=Scale(zero=false))
      .encodeColor(
         field="title",
         dataType=Nominal,
         legend=Legend(orient="top-left", offset=30, title=title))
      .show
  }
  
  
  /**
   * Convenient plot wrapper.  This is for backward-compatibility.  It does a log-y plot.
   */
  def multiLineLogPlot(data: Array[Map[String,Any]], plotname:String = "my plot", title:String = "data series") {
    multiLineLogYPlot(data, plotname, title)
  }
  
  
  /**
   * A convenient wrapper for a particular style of Vegas plot.
   * This is intended to be called with the result of the vegasData() function above.
   * 
   * The data points (each is  Map) should have the data labeld by "x" and "y",
   * and each point should have an entry for "title" which will be used for
   * associating the data series and labeling them.
   */
  def multiLinePlot(data: Array[Map[String,Any]], plotname:String = "my plot", title:String = "data series") {
    if (data.length < 2) {
      println("ERROR: multiLineLogPlot() - not enough data for plotting.")
      return
    }
    Vegas(plotname, width=plot_size_x, height=plot_size_y)
    .withData(data)
    .mark(Line)
    .encodeX("x", Quant, scale=Scale(zero=false))
    .encodeY("y", Quant, scale=Scale(zero=false))
    .encodeColor(
       field="title",
       dataType=Nominal,
       legend=Legend(orient="top-left", offset=30, title=title))
    .show
  }
  
  
  /**
   * A convenient wrapper for a particular style of Vegas plot.
   * This is intended to be called with the result of the experimentPaths()
   * function above.
   */
  def plotExpPath(data: Array[Map[String,Any]], title:String = "") {
    if (data.length < 2) {
      println("ERROR: plotExpPath() - not enoug data for plotting")
      return
    }
    
    Vegas("Experiment Path: "+title, width=plot_size_x, height=plot_size_y)
    .withData(data)
    .mark(Point)
    .encodeX("pktnum", Quant, scale=Scale(zero=false))
    .encodeY("t", Quant, scale=Scale(zero=false))
    .encodeColor(
       field="ip",
       dataType=Nominal,
       legend=Legend(orient="top-left", offset=30, title="timestamp"))
    .encodeDetailFields(Field(field="symbol", dataType=Nominal))
    .show
  }

  
  
  
}