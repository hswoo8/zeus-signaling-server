function renderAdmissionPage(nonce, state) {
    const data = JSON.stringify(state).replace(/</g, '\\u003c');
    return `<!doctype html><html lang="ko"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>입장 정원·대기열</title>
<style>body{font:16px/1.6 system-ui;max-width:900px;margin:30px auto;padding:20px;background:#111b25;color:#e4eaf1}a{color:#b8d5ed}label{display:block;margin:16px 0}input{font:inherit;max-width:90%;padding:6px}button{font:inherit;padding:10px 20px}#state{white-space:pre-line;padding:16px;background:#213244}small{display:block;color:#c2cbd5}#message{white-space:pre-line}</style>
<a href="/admin">관리자 홈</a><h1>입장 정원·대기열</h1><p>정원을 낮춰도 연결된 이용자와 진행 중 대전을 끊지 않습니다. 줄어든 정원 아래로 내려갈 때까지 신규 입장만 기다립니다. 구버전·최신 버전 풀은 각각 조절합니다.</p>
<div id="state">불러오는 중</div><p id="observations"></p><form id="settings">
<label>수동 최대 접속 수 <input id="manualLimit" type="number" min="0" required></label>
<small>0은 신규 입장 일시 중지입니다. 서버·DB를 종료하지 않습니다.</small>
<label><input id="budgetEnabled" type="checkbox"> 실측 비용 모델로 정원 자동 계산</label>
<label>전체 서비스 월 예산 목표(USD) <input id="budgetUsd" type="number" min="0.01" step="0.01" required></label>
<label>운영·Beta 합산 기본 월 비용(USD) <input id="baselineMonthlyUsd" type="number" min="0" step="0.01"></label>
<label>평균 동시 접속 100명당 추가 월 비용(USD) <input id="costPer100MonthlyUsd" type="number" min="0.01" step="0.01"></label>
<label>측정 시각(ISO 8601) <input id="measuredAt" type="text" placeholder="2026-10-02T00:00:00Z"></label>
<small>최대 접속 100명이 아닌, 평균 100명이 한 달 동안 유지되는 기준입니다. 기본 비용에는 Beta·DB·라우터를 포함하고 증가분은 운영 서버 사용량으로 구합니다. 추측값을 실측값으로 입력하지 마세요. 비용 모델이 없으면 수동 정원을 유지합니다. 정원 계산은 이 인원이 한 달 내내 접속한다는 보수적 가정입니다. Railway 청구액을 자동 조회하거나 서버를 종료하지 않습니다. 모델은 최종 청구액을 보장하지 않습니다.</small>
<p><button type="submit">저장 · 재시작 없이 적용</button> <button type="button" id="refresh">새로고침</button></p></form><p id="message" role="status"></p>
<script nonce="${nonce}">let current=${data};const ids=['manualLimit','budgetEnabled','budgetUsd','baselineMonthlyUsd','costPer100MonthlyUsd','measuredAt'];
function draw(s){current=s;if(!s){document.getElementById('state').textContent='인증과 서버 정원 설정이 필요합니다.';document.getElementById('settings').hidden=true;return}document.getElementById('state').textContent='접속 '+s.connected+'명 · 입장 예약 '+s.reserved+'명 · 대기 '+s.queued+'명\\n적용 정원 '+s.effectiveLimit+' / 서버 상한 '+s.hardLimit+' · 전체 예산 정원 배분 '+Math.round(s.poolShare*100)+'%\\n비용 모델: '+({manual:'수동 조절',measured_model:'실측 모델 사용 중',waiting_for_measurement:'실측값 대기'}[s.budgetStatus]||s.budgetStatus);document.getElementById('observations').textContent=s.observations?.averageConnections!=null?'최근 관측 '+s.observations.coveredHours.toFixed(1)+'시간 · 평균 '+s.observations.averageConnections.toFixed(1)+'명 · 최대 '+s.observations.peakConnections+'명 (5분마다 저장, 최근 7일)':'접속량 관측 중 · 첫 기록은 약 5분 뒤 표시됩니다.';document.getElementById('manualLimit').max=s.hardLimit;for(const id of ids){const e=document.getElementById(id);if(e.type==='checkbox')e.checked=s.settings[id];else e.value=s.settings[id]??''}}
async function refresh(){const r=await fetch('/admin/api/admission',{cache:'no-store'});if(!r.ok)throw Error('상태 조회 실패');draw(await r.json())}
document.getElementById('refresh').onclick=()=>refresh().catch(e=>document.getElementById('message').textContent=e.message);
document.getElementById('settings').onsubmit=async e=>{e.preventDefault();const body={};for(const id of ids){const f=document.getElementById(id);body[id]=f.type==='checkbox'?f.checked:f.value===''?null:id==='measuredAt'?f.value:Number(f.value)}try{const r=await fetch('/admin/api/admission',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});if(!r.ok)throw Error('저장 실패: 범위와 실측 시각을 확인해주세요.');draw(await r.json());document.getElementById('message').textContent='저장했습니다. 기존 대전은 유지됩니다.'}catch(error){document.getElementById('message').textContent=error.message}};draw(current);</script></html>`;
}

module.exports = { renderAdmissionPage };
