<#assign title="Items">
<#include "console_header.ftl">
<script>
function elem(id) {
    return document.getElementById(id);
}

function resetTimeTo() {
    elem("to").value = "";
}

function fromOffsetFromBottom() {
    elem("from_offset").value = elem("from_offset_bottom").innerHTML;
}

function magicFromBottom() {
    elem("magic").value = elem("magic_bottom").innerHTML;
}
</script>

<form method="GET">
    <table>
        <tr>
            <td>
                <label for="symbol">Symbol:</label>
                <input type="text" name="symbol" id="symbol" value="${request.symbol!""}" />
            </td>
        </tr>
        <tr>
            <td>
                <label for="from">Time (UTC) from:</label>
                <input type="text" name="from" id="from" value="<#if is_continue_request == false>${request.timeFrom}</#if>" size="24" />
                <label for="to">To:</label>
                <input type="text" name="to" id="to" value="${request.timeTo}" size="24" />
                <button type="button" onclick="resetTimeTo(); return false;">X</button>
            </td>
        </tr>
        <tr>
            <td>
                <label for="from_offset">Continue from offset:</label>
                <input type="text" name="from_offset" id="from_offset" value="" size="12" />
                <button type="button" onclick="fromOffsetFromBottom(); return false;">^</button>
                <label for="magic">Magic:</label>
                <input type="text" name="magic" id="magic" value="${magic!""}" size="32" />
                <button type="button" onclick="magicFromBottom(); return false;">^</button>
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
<table class="blueTable">
    <thead>
        <tr>
            <th>Symbol</th>
            <th>Time (ms)</th>
            <th>Time</th>
            <th>Value</th>
            <th>Volume</th>
        </tr>
    </thead>
    <tbody>
        <#list rows as row>
        <tr>
            <td>${row.symbol}</td>
            <td>${row.timeMillis?c}</td>
            <td>${row.time}</td>
            <td>${row.value}</td>
            <td>${row.volume}</td>
        </tr>
        <#if !(row?has_next)>
        <#assign next_offset = row.offset + 1 >
        <tr>
            <td colspan="5" align="right">
            <table class="noBorder" cellpadding="0" cellspacing="0">
            <tr>
                <td align="right">Continue from offset:</td>
                <td><div id="from_offset_bottom">${next_offset?c}</div></td>
                <td><button type="button" onclick="fromOffsetFromBottom(); return false;">^</button></td>
            </tr>
            <tr>
                <td align="right">Magic word:</td>
                <td><div id="magic_bottom">${magic}</div></td>
                <td><button type="button" onclick="magicFromBottom(); return false;">^</button></td>
            </tr>
            </table>
            </td>
        </tr>
        </#if>
        </#list>
    </tbody>
</table>
</#if>

<#include "console_footer.ftl">