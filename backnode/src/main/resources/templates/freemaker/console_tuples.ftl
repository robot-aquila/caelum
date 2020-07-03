<#assign title="Tuples">
<#include "console_header.ftl">
<script>
function resetTimeTo() {
	document.getElementById("to").value = "";
}
</script>

<form method="GET">
	<table>
		<tr>
			<td>
				<label for="symbol">Symbol:</label>
				<input type="text" name="symbol" id="symbol" value="${request.symbol!""}" />
				<label for="period">Period:</label>
				<select name="period" id="period">
				<#list periods as p>
					<option value="${p}"<#if p == request.period> selected</#if>/>${p}</option>
				</#list>
				</select>
			</td>
		</tr>
		<tr>
			<td>
				<label for="from">Time (UTC) from:</label>
				<input type="text" name="from" id="from" value="${request.timeFrom}" size="24" />
				<label for="to">To:</label>
				<input type="text" name="to" id="to" value="${request.timeTo}" size="24" />
				<button type="button" onclick="resetTimeTo(); return false;">X</button>
			</td>
		</tr>
		<tr>
			<td>
				<label for="limit">Limit:</label>
				<input type="text" name="limit" id="limit" value="${request.limit?c}" />
				<input type="submit" value="fetch" />
			</td>
		</tr>
	</table>
</form>

<#if rows??>
Shown data for ${request.symbol} aggregated by ${request.period} from ${request.timeFrom} to ${request.timeTo} with limit of ${request.limit}<p>
<table class="blueTable">
	<thead>
		<tr>
			<th>Time (ms)</th>
			<th>Time</th>
			<th>Open</th>
			<th>High</th>
			<th>Low</th>
			<th>Close</th>
			<th>Volume</th>
		</tr>
	</thead>
	<tbody>
		<#list rows as row>
		<tr>
			<td>${row.time?c}</td>
			<td>${row.timeAsInstant}</td>
			<td>${row.open}</td>
			<td>${row.high}</td>
			<td>${row.low}</td>
			<td>${row.close}</td>
			<td>${row.volume}</td>
		</tr>
		</#list>
	</tbody>
</table>
</#if>

<#include "console_footer.ftl">