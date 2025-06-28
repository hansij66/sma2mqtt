function setFloatingTableHeader() {
	$("div.floatingTable").each(function() {
		var Genuin = $(".headGenuin", this);
		var Float = $(".headFloat", this);

		for (n = 0; n<Genuin.children().length; n++) {
			Float.children()[n].setAttribute("class",Genuin.children()[n].getAttribute("class"));
			
		}
		
		var yScroll = $(window).scrollTop();
		var yView = $(this).offset();
		var yTop = Float.offset().top -  $(this).offset().top;
		Float.css("width", Genuin.width());

		var yNew = Math.min(yScroll - yView.top, $(this).height() - Float.height())
		if (yTop > yNew) {Float.css("transition-duration", "0ms");}
		else {Float.css("transition-duration", "0ms");}
			
		if ((yScroll > yView.top) && (yScroll < yView.top + $(this).height())) {
			Float.css("visibility", "visible");
			Float.css("top", yNew + "px");
			
			//Float.css("width", Float.width());

			$("th", Float).each(function(index) {
				rowWidth = $("th", Genuin).eq(index).css('width')
				$(this).css('width', rowWidth);
			})
		
		}
		else {
			Float.css("visibility", "hidden");
			Float.css("top", "0px");
		}
	});
}
function createFloatingTableHeader() {
		$("table.tableWithFloatingHeader").each(function() {
			$(this).wrap("<div class=\"floatingTable\"></div>");

			var Genuin = $("tr:first", this)
			Genuin.before(Genuin.clone());
			var Float = $("tr:first", this)

			Float.addClass("headFloat");
			Float.css("visibility", "hidden");

			Genuin.addClass("headGenuin");
		});
	setFloatingTableHeader();
	$(window).scroll(setFloatingTableHeader);
	$(window).resize(setFloatingTableHeader);
}
